#include "storage/ducklake_stats_copy.hpp"

#include "common/ducklake_util.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/file_system.hpp"

#include <cmath>

namespace duckdb {

namespace {

//! Bounds are only recorded for types whose physical-value order matches their
//! logical order and whose values round-trip through the stats VARCHAR
//! representation. Everything else still gets null/value counts.
bool TypeSupportsMinMaxStats(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}
}

//! VARCHAR bounds above this length are dropped (a truncated max is not an
//! upper bound; rather than truncate-and-increment like parquet, drop bounds).
constexpr idx_t MAX_STRING_STATS_LENGTH = 1024;

template <class T>
bool ValueIsNan(T) {
	return false;
}
template <>
bool ValueIsNan(float v) {
	return std::isnan(v);
}
template <>
bool ValueIsNan(double v) {
	return std::isnan(v);
}

//! Per-column statistics accumulated from sunk chunks.
struct StatsAccumulator {
	explicit StatsAccumulator(LogicalType type_p)
	    : type(std::move(type_p)), minmax_supported(TypeSupportsMinMaxStats(type)),
	      is_float(type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE) {
	}

	LogicalType type;
	bool minmax_supported;
	bool is_float;
	//! Cleared when a bound cannot be represented (oversized string)
	bool minmax_valid = true;
	idx_t null_count = 0;
	idx_t num_values = 0;
	bool contains_nan = false;
	//! NULL Values until the first non-null (non-NaN) value is seen
	Value min_val;
	Value max_val;

	void Update(Vector &vec, idx_t count);
	void Merge(const StatsAccumulator &other);

private:
	void UpdateBound(const Value &candidate, bool is_min);
	template <class T>
	void TemplatedArgMinMax(const UnifiedVectorFormat &vdata, idx_t count, optional_idx &argmin, optional_idx &argmax);
};

template <class T>
void StatsAccumulator::TemplatedArgMinMax(const UnifiedVectorFormat &vdata, idx_t count, optional_idx &argmin,
                                          optional_idx &argmax) {
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	optional_idx min_row, max_row;
	T min_v {};
	T max_v {};
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		T v = data[idx];
		if (ValueIsNan<T>(v)) {
			contains_nan = true;
			continue;
		}
		if (!min_row.IsValid() || LessThan::Operation<T>(v, min_v)) {
			min_v = v;
			min_row = i;
		}
		if (!max_row.IsValid() || GreaterThan::Operation<T>(v, max_v)) {
			max_v = v;
			max_row = i;
		}
	}
	argmin = min_row;
	argmax = max_row;
}

void StatsAccumulator::UpdateBound(const Value &candidate, bool is_min) {
	if (candidate.type().id() == LogicalTypeId::VARCHAR &&
	    StringValue::Get(candidate).size() > MAX_STRING_STATS_LENGTH) {
		minmax_valid = false;
		min_val = Value();
		max_val = Value();
		return;
	}
	auto &bound = is_min ? min_val : max_val;
	if (bound.IsNull()) {
		bound = candidate;
		return;
	}
	if (is_min ? candidate < bound : candidate > bound) {
		bound = candidate;
	}
}

void StatsAccumulator::Update(Vector &vec, idx_t count) {
	num_values += count;

	UnifiedVectorFormat vdata;
	vec.ToUnifiedFormat(count, vdata);
	if (!vdata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (!vdata.validity.RowIsValid(vdata.sel->get_index(i))) {
				null_count++;
			}
		}
	}
	if (!minmax_supported || !minmax_valid) {
		if (!is_float) {
			return;
		}
	}

	optional_idx argmin, argmax;
	switch (vec.GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedArgMinMax<bool>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::INT8:
		TemplatedArgMinMax<int8_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::INT16:
		TemplatedArgMinMax<int16_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::INT32:
		TemplatedArgMinMax<int32_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::INT64:
		TemplatedArgMinMax<int64_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::INT128:
		TemplatedArgMinMax<hugeint_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::UINT8:
		TemplatedArgMinMax<uint8_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::UINT16:
		TemplatedArgMinMax<uint16_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::UINT32:
		TemplatedArgMinMax<uint32_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::UINT64:
		TemplatedArgMinMax<uint64_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::UINT128:
		TemplatedArgMinMax<uhugeint_t>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::FLOAT:
		TemplatedArgMinMax<float>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::DOUBLE:
		TemplatedArgMinMax<double>(vdata, count, argmin, argmax);
		break;
	case PhysicalType::VARCHAR:
		TemplatedArgMinMax<string_t>(vdata, count, argmin, argmax);
		break;
	default:
		// unreachable given TypeSupportsMinMaxStats, except float NaN scans
		return;
	}
	if (!minmax_supported || !minmax_valid) {
		// float column that only needed the NaN scan
		return;
	}
	if (argmin.IsValid()) {
		UpdateBound(vec.GetValue(argmin.GetIndex()), true);
	}
	if (argmax.IsValid() && minmax_valid) {
		UpdateBound(vec.GetValue(argmax.GetIndex()), false);
	}
}

void StatsAccumulator::Merge(const StatsAccumulator &other) {
	null_count += other.null_count;
	num_values += other.num_values;
	contains_nan = contains_nan || other.contains_nan;
	if (!other.minmax_valid) {
		minmax_valid = false;
		min_val = Value();
		max_val = Value();
	}
	if (!minmax_valid) {
		return;
	}
	if (!other.min_val.IsNull()) {
		UpdateBound(other.min_val, true);
	}
	if (!other.max_val.IsNull() && minmax_valid) {
		UpdateBound(other.max_val, false);
	}
}

struct StatsCopyBindData : public FunctionData {
	CopyFunction inner_function;
	unique_ptr<FunctionData> inner;
	vector<string> names;
	vector<LogicalType> types;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<StatsCopyBindData>();
		result->inner_function = inner_function;
		result->inner = inner->Copy();
		result->names = names;
		result->types = types;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StatsCopyBindData>();
		return names == other.names && types == other.types && inner->Equals(*other.inner);
	}

	StatsCopyBindData() : inner_function("ducklake_stats_copy_wrapper") {
	}
};

struct StatsCopyGlobalState : public GlobalFunctionData {
	unique_ptr<GlobalFunctionData> inner;
	string file_path;
	mutex merge_lock;
	vector<StatsAccumulator> stats;
	idx_t row_count = 0;
	optional_ptr<CopyFunctionFileStatistics> target;
};

struct StatsCopyLocalState : public LocalFunctionData {
	unique_ptr<LocalFunctionData> inner;
	vector<StatsAccumulator> stats;
	idx_t row_count = 0;
};

vector<StatsAccumulator> MakeAccumulators(const vector<LogicalType> &types) {
	vector<StatsAccumulator> result;
	result.reserve(types.size());
	for (auto &type : types) {
		result.emplace_back(type);
	}
	return result;
}

unique_ptr<GlobalFunctionData> StatsCopyInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                         const string &file_path) {
	auto &bind_data = bind_data_p.Cast<StatsCopyBindData>();
	auto result = make_uniq<StatsCopyGlobalState>();
	result->inner = bind_data.inner_function.copy_to_initialize_global(context, *bind_data.inner, file_path);
	result->file_path = file_path;
	result->stats = MakeAccumulators(bind_data.types);
	return std::move(result);
}

unique_ptr<LocalFunctionData> StatsCopyInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<StatsCopyBindData>();
	auto result = make_uniq<StatsCopyLocalState>();
	result->inner = bind_data.inner_function.copy_to_initialize_local(context, *bind_data.inner);
	result->stats = MakeAccumulators(bind_data.types);
	return std::move(result);
}

void StatsCopyGetWrittenStatistics(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                   CopyFunctionFileStatistics &statistics) {
	auto &global_state = gstate.Cast<StatsCopyGlobalState>();
	global_state.target = &statistics;
}

void StatsCopySink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                   LocalFunctionData &lstate, DataChunk &input) {
	auto &bind_data = bind_data_p.Cast<StatsCopyBindData>();
	auto &local_state = lstate.Cast<StatsCopyLocalState>();
	auto &global_state = gstate.Cast<StatsCopyGlobalState>();
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		local_state.stats[c].Update(input.data[c], input.size());
	}
	local_state.row_count += input.size();
	bind_data.inner_function.copy_to_sink(context, *bind_data.inner, *global_state.inner, *local_state.inner, input);
}

void StatsCopyCombine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                      LocalFunctionData &lstate) {
	auto &bind_data = bind_data_p.Cast<StatsCopyBindData>();
	auto &local_state = lstate.Cast<StatsCopyLocalState>();
	auto &global_state = gstate.Cast<StatsCopyGlobalState>();
	{
		lock_guard<mutex> guard(global_state.merge_lock);
		for (idx_t c = 0; c < global_state.stats.size(); c++) {
			global_state.stats[c].Merge(local_state.stats[c]);
		}
		global_state.row_count += local_state.row_count;
	}
	if (bind_data.inner_function.copy_to_combine) {
		bind_data.inner_function.copy_to_combine(context, *bind_data.inner, *global_state.inner, *local_state.inner);
	}
}

void StatsCopyFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate) {
	auto &bind_data = bind_data_p.Cast<StatsCopyBindData>();
	auto &global_state = gstate.Cast<StatsCopyGlobalState>();
	if (bind_data.inner_function.copy_to_finalize) {
		bind_data.inner_function.copy_to_finalize(context, *bind_data.inner, *global_state.inner);
	}
	if (!global_state.target) {
		return;
	}
	auto &statistics = *global_state.target;
	statistics.row_count = global_state.row_count;
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(global_state.file_path, FileFlags::FILE_FLAGS_READ);
	statistics.file_size_bytes = NumericCast<idx_t>(fs.GetFileSize(*handle));
	// footer_size_bytes stays NULL: the format's footer size is not surfaced here

	for (idx_t c = 0; c < global_state.stats.size(); c++) {
		auto &acc = global_state.stats[c];
		case_insensitive_map_t<Value> column_stats;
		column_stats["null_count"] = Value::UBIGINT(acc.null_count);
		column_stats["num_values"] = Value::UBIGINT(acc.num_values);
		if (acc.minmax_valid && !acc.min_val.IsNull()) {
			column_stats["min"] = acc.min_val;
		}
		if (acc.minmax_valid && !acc.max_val.IsNull()) {
			column_stats["max"] = acc.max_val;
		}
		if (acc.is_float) {
			column_stats["has_nan"] = Value::BOOLEAN(acc.contains_nan);
		}
		vector<string> name_path {bind_data.names[c]};
		statistics.column_statistics[DuckLakeUtil::ToQuotedList(name_path)] = std::move(column_stats);
	}
}

} // namespace

bool DuckLakeStatsCopy::NeedsWrapping(const CopyFunction &fn) {
	return !fn.copy_to_get_written_statistics;
}

CopyFunction DuckLakeStatsCopy::WrapFunction(const CopyFunction &inner) {
	if (inner.prepare_batch || inner.flush_batch || inner.rotate_files || inner.rotate_next_file) {
		throw NotImplementedException(
		    "DuckLakeStatsCopy only supports single-file sink-based COPY functions");
	}
	D_ASSERT(NeedsWrapping(inner));
	CopyFunction result(inner.name + "_with_stats");
	// binding happens on the INNER function; the caller wraps the bound data
	// through WrapBindData, so the wrapper needs no copy_to_bind of its own
	result.copy_to_initialize_global = StatsCopyInitializeGlobal;
	result.copy_to_initialize_local = StatsCopyInitializeLocal;
	result.copy_to_get_written_statistics = StatsCopyGetWrittenStatistics;
	result.copy_to_sink = StatsCopySink;
	result.copy_to_combine = StatsCopyCombine;
	result.copy_to_finalize = StatsCopyFinalize;
	result.execution_mode = inner.execution_mode;
	result.initialize_operator = inner.initialize_operator;
	return result;
}

unique_ptr<FunctionData> DuckLakeStatsCopy::WrapBindData(const CopyFunction &inner,
                                                         unique_ptr<FunctionData> inner_bind, vector<string> names,
                                                         vector<LogicalType> types) {
	auto result = make_uniq<StatsCopyBindData>();
	result->inner_function = inner;
	result->inner = std::move(inner_bind);
	result->names = std::move(names);
	result->types = std::move(types);
	return std::move(result);
}

} // namespace duckdb
