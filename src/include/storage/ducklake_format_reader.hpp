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

//! How an output column is produced.
enum class FormatReaderColumn : uint8_t {
	FILE_COLUMN, //! read from the file (see output_source_index)
	ORDINAL,     //! file_row_number, emitted by the reader
	MISSING      //! not in the file -> NULL
};

//! Reads a single non-parquet DuckLake data file ("vortex", "json", ...) by driving its read_<format>
//! table function and adapting the chunks to DuckLake's MultiFileReader contract (column mapping,
//! delete filters, pushed-down filters, file_row_number), like the parquet reader path does.
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
	static TableFunction GetReadFunction(ClientContext &context, const string &format);

	mutex lock;
	DuckLakeFunctionInfo &read_info;
	string format; //! selects the read_<format> function
	bool initialized_scan = false;
	int64_t file_row_number = 0;

	//! read_<format> scan bound to the file
	TableFunction file_scan;
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> file_types;
	vector<string> file_names;

	unique_ptr<ThreadContext> thread_context;
	unique_ptr<ExecutionContext> execution_context;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;

	vector<column_t> scan_column_ids;              //! columns projected out of the file
	DataChunk scan_chunk;                          //! raw columns read from the file
	vector<FormatReaderColumn> output_kinds;       //! how each output column is produced
	vector<idx_t> output_source_index;             //! for FILE_COLUMN: position in scan_chunk
};

} // namespace duckdb
