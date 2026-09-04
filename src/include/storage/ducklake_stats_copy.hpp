//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_stats_copy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/copy_function.hpp"

namespace duckdb {

//! Wrap a sink-based COPY function so per-file column statistics (min/max,
//! null_count, num_values, has_nan) are computed from the chunks as they
//! stream through the sink — the same point where the parquet writer computes
//! its statistics — and reported through copy_to_get_written_statistics /
//! CopyFunctionReturnType::WRITTEN_FILE_STATISTICS.
//!
//! For COPY functions that do not report written statistics themselves (the
//! vortex COPY returns only a file list). Costs one comparison pass over each
//! chunk at write time; no re-read of the written file. Single-file sink-based
//! writes only: the wrapped function must not use the batch or rotation APIs.
struct DuckLakeStatsCopy {
	//! Whether `fn` needs wrapping to report written statistics
	static bool NeedsWrapping(const CopyFunction &fn);
	//! The wrapping COPY function forwarding to `inner`
	static CopyFunction WrapFunction(const CopyFunction &inner);
	//! Wrap `inner`'s bound data; `names`/`types` describe the sunk chunks
	static unique_ptr<FunctionData> WrapBindData(const CopyFunction &inner, unique_ptr<FunctionData> inner_bind,
	                                             vector<string> names, vector<LogicalType> types);
};

} // namespace duckdb
