//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_vortex_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
struct DuckLakeFunctionInfo;

//! Marks how a single output column of the DuckLakeVortexReader is produced.
enum class VortexReaderColumn : uint8_t {
	//! Projected directly from the underlying read_vortex scan (see source_index)
	FILE_COLUMN,
	//! Ordinal position of the row within the file (row_id_start + file_row_number is applied by the
	//! MultiFileReader on top). Emitted by the reader itself.
	ORDINAL,
	//! Column that is not present in the vortex file - emit its default value (usually NULL)
	MISSING
};

//! Reads a single DuckLake data file stored in the Vortex format. It drives the vortex extension's
//! `read_vortex` table function for the physical file and adapts the produced chunks to DuckLake's
//! MultiFileReader contract (column mapping, positional delete filters, pushed-down filters and the
//! file_row_number virtual column) exactly like the parquet reader path does.
class DuckLakeVortexReader : public BaseFileReader {
public:
	DuckLakeVortexReader(DuckLakeFunctionInfo &read_info, const OpenFileInfo &info,
	                     vector<MultiFileColumnDefinition> columns);

public:
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	string GetReaderType() const override;
	void AddVirtualColumn(column_t virtual_column_id) override;

private:
	//! Look up the vortex extension `read_vortex` table function from the catalog.
	static TableFunction GetReadVortexFunction(ClientContext &context);

	mutex lock;
	DuckLakeFunctionInfo &read_info;
	bool initialized_scan = false;
	int64_t file_row_number = 0;

	//! Underlying read_vortex scan bound to the physical file.
	TableFunction vortex_scan;
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> file_types;
	vector<string> file_names;

	unique_ptr<ThreadContext> thread_context;
	unique_ptr<ExecutionContext> execution_context;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;

	//! Column ids projected out of the vortex file (indexes into file_names/file_types).
	vector<column_t> scan_column_ids;
	//! Chunk holding the raw columns read from read_vortex (in scan_column_ids order).
	DataChunk scan_chunk;
	//! For each output column: how it is produced.
	vector<VortexReaderColumn> output_kinds;
	//! For FILE_COLUMN outputs: the position within scan_chunk to reference.
	vector<idx_t> output_source_index;
};

} // namespace duckdb
