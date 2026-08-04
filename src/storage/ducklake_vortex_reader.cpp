#include "storage/ducklake_vortex_reader.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_delete_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

DuckLakeVortexReader::DuckLakeVortexReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info,
                                           vector<MultiFileColumnDefinition> columns_p)
    : BaseFileReader(info), read_info(read_info) {
	columns = std::move(columns_p);
}

TableFunction DuckLakeVortexReader::GetReadVortexFunction(ClientContext &context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	// The vortex extension registers read_vortex on load; auto-load it if it is available.
	ExtensionHelper::TryAutoLoadExtension(instance, "vortex");
	ExtensionLoader loader(instance, "ducklake");
	auto entry = loader.TryGetTableFunction("read_vortex");
	if (!entry) {
		throw MissingExtensionException("Reading DuckLake data files stored in the 'vortex' format requires the "
		                                "\"vortex\" extension to be installed and loaded");
	}
	auto &vortex_scan_entry = entry->Cast<TableFunctionCatalogEntry>();
	return vortex_scan_entry.functions.GetFunctionByOffset(0);
}

bool DuckLakeVortexReader::TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
                                             LocalTableFunctionState &lstate) {
	{
		// only one thread reads a given file
		lock_guard<mutex> guard(lock);
		if (initialized_scan) {
			return false;
		}
		initialized_scan = true;
	}

	// Bind read_vortex on the physical file to discover its schema.
	vortex_scan = GetReadVortexFunction(context);

	vector<Value> children;
	children.push_back(Value(file.path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.SetName("DuckLakeVortexReader");
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	bind_data = vortex_scan.bind(context, bind_input, file_types, file_names);

	// Build the projection into the vortex file and the mapping to the reader's output columns.
	vector<LogicalType> scan_types;
	for (auto &column_id : column_indexes) {
		auto index = column_id.GetPrimaryIndex();
		auto &col = columns[index];
		// Virtual columns carry an INTEGER identifier. The only one the reader must materialize itself
		// is the ordinal (file_row_number); higher-level virtuals (row_id / snapshot_id) are computed by
		// the MultiFileReader as expressions on top of the ordinal.
		if (!col.identifier.IsNull() && col.identifier.type().id() == LogicalTypeId::INTEGER &&
		    IntegerValue::Get(col.identifier) == MultiFileReader::ORDINAL_FIELD_ID) {
			output_kinds.push_back(VortexReaderColumn::ORDINAL);
			output_source_index.push_back(0);
			continue;
		}
		// Regular column - locate it in the vortex file by name.
		auto column_name = col.name.GetIdentifierName();
		optional_idx file_position;
		for (idx_t i = 0; i < file_names.size(); i++) {
			if (StringUtil::CIEquals(file_names[i], column_name)) {
				file_position = i;
				break;
			}
		}
		if (!file_position.IsValid()) {
			// column is not present in this file - emit its default (NULL)
			output_kinds.push_back(VortexReaderColumn::MISSING);
			output_source_index.push_back(0);
			continue;
		}
		output_kinds.push_back(VortexReaderColumn::FILE_COLUMN);
		output_source_index.push_back(scan_column_ids.size());
		scan_column_ids.push_back(file_position.GetIndex());
		scan_types.push_back(file_types[file_position.GetIndex()]);
	}

	// read_vortex needs at least one projected column to determine the cardinality of each chunk.
	if (scan_column_ids.empty() && !file_types.empty()) {
		scan_column_ids.push_back(0);
		scan_types.push_back(file_types[0]);
	}
	scan_chunk.Initialize(context, scan_types);

	thread_context = make_uniq<ThreadContext>(context);
	execution_context = make_uniq<ExecutionContext>(context, *thread_context, nullptr);
	TableFunctionInitInput init_input(bind_data.get(), scan_column_ids, vector<idx_t>(), nullptr);
	global_state = vortex_scan.init_global(context, init_input);
	local_state = vortex_scan.init_local(*execution_context, init_input, global_state.get());
	return true;
}

AsyncResult DuckLakeVortexReader::Scan(ClientContext &context, GlobalTableFunctionState &global_table_state,
                                       LocalTableFunctionState &local_table_state, DataChunk &chunk) {
	scan_chunk.Reset();
	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	vortex_scan.function(context, function_input, scan_chunk);
	idx_t scan_count = scan_chunk.size();
	if (scan_count == 0) {
		return AsyncResult(SourceResultType::FINISHED);
	}

	for (idx_t c = 0; c < output_kinds.size(); c++) {
		switch (output_kinds[c]) {
		case VortexReaderColumn::FILE_COLUMN: {
			auto &source = scan_chunk.data[output_source_index[c]];
			if (chunk.data[c].GetType() != source.GetType()) {
				VectorOperations::Cast(context, source, chunk.data[c], scan_count);
			} else {
				chunk.data[c].Reference(source);
			}
			break;
		}
		case VortexReaderColumn::ORDINAL: {
			auto ordinal_data = FlatVector::GetDataMutable<int64_t>(chunk.data[c]);
			for (idx_t r = 0; r < scan_count; r++) {
				ordinal_data[r] = file_row_number + NumericCast<int64_t>(r);
			}
			break;
		}
		case VortexReaderColumn::MISSING:
			chunk.data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[c], true);
			break;
		}
	}
	chunk.SetCardinality(scan_count);

	if (filters || deletion_filter) {
		SelectionVector sel;
		idx_t approved_tuple_count = scan_count;
		if (deletion_filter) {
			// delete filters are positional within the file (row_id_start + file_row_number)
			approved_tuple_count = deletion_filter->Filter(file_row_number, approved_tuple_count, sel);
		}
		if (filters) {
			for (auto &entry : *filters) {
				auto &filter = entry.Filter();
				if (ExpressionFilter::IsRootOptionalFilter(filter)) {
					continue;
				}
				auto column_id = entry.GetIndex().GetIndex();
				auto &vec = chunk.data[column_id];
				UnifiedVectorFormat vdata;
				vec.ToUnifiedFormat(vdata);
				auto filter_state = TableFilterState::Initialize(context, filter);
				approved_tuple_count = ColumnSegment::FilterSelection(sel, vec, vdata, filter, *filter_state,
				                                                      chunk.size(), approved_tuple_count);
			}
		}
		if (approved_tuple_count != chunk.size()) {
			chunk.Slice(sel, approved_tuple_count);
		}
	}

	file_row_number += NumericCast<int64_t>(scan_count);
	return AsyncResult(SourceResultType::HAVE_MORE_OUTPUT);
}

void DuckLakeVortexReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		columns.back().identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
	} else {
		throw InternalException("Unsupported virtual column id %d for vortex reader", virtual_column_id);
	}
}

string DuckLakeVortexReader::GetReaderType() const {
	return "DuckLake Vortex";
}

} // namespace duckdb
