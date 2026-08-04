//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_format_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
struct DuckLakeFunctionInfo;

//! Marks how a single output column of the DuckLakeFormatReader is produced.
enum class FormatReaderColumn : uint8_t {
	//! Projected directly from the underlying read_<format> scan (see source_index)
	FILE_COLUMN,
	//! Ordinal position of the row within the file (row_id_start + file_row_number is applied by the
	//! MultiFileReader on top). Emitted by the reader itself.
	ORDINAL,
	//! Column that is not present in the file - emit its default value (usually NULL)
	MISSING
};

//! Reads a single DuckLake data file stored in a non-parquet format (e.g. "vortex", "json"). It drives
//! the format's `read_<format>` table function for the physical file and adapts the produced chunks to
//! DuckLake's MultiFileReader contract (column mapping, positional delete filters, pushed-down filters
//! and the file_row_number virtual column) exactly like the parquet reader path does.
class DuckLakeFormatReader : public BaseFileReader {
public:
	DuckLakeFormatReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info, string format,
	                     vector<MultiFileColumnDefinition> columns);

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	string GetReaderType() const override;
	void AddVirtualColumn(column_t virtual_column_id) override;

private:
	//! Look up the `read_<format>` table function (auto-loading the format's extension if available).
	static TableFunction GetReadFunction(ClientContext &context, const string &format);

	mutex lock;
	DuckLakeFunctionInfo &read_info;
	//! The physical format of the file ("vortex", "json", ...); selects the read_<format> function.
	string format;
	bool initialized_scan = false;
	int64_t file_row_number = 0;

	//! Underlying read_<format> scan bound to the physical file.
	TableFunction file_scan;
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> file_types;
	vector<string> file_names;

	unique_ptr<ThreadContext> thread_context;
	unique_ptr<ExecutionContext> execution_context;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;

	//! Column ids projected out of the file (indexes into file_names/file_types).
	vector<column_t> scan_column_ids;
	//! Chunk holding the raw columns read from read_<format> (in scan_column_ids order).
	DataChunk scan_chunk;
	//! For each output column: how it is produced.
	vector<FormatReaderColumn> output_kinds;
	//! For FILE_COLUMN outputs: the position within scan_chunk to reference.
	vector<idx_t> output_source_index;
};

} // namespace duckdb
