/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2026, Tarantool AUTHORS, please see AUTHORS file.
 */
#ifndef TARANTOOL_LIB_ANN_NUMERIC_H_INCLUDED
#define TARANTOOL_LIB_ANN_NUMERIC_H_INCLUDED

#include "ann_backend.h"

#if defined(__cplusplus)
extern "C" {
#endif

/** Convert a finite number to float32 with ties-to-even rounding. */
enum ann_status
ann_f32_from_double(double value, float *out);

/** Canonicalize an input vector, then validate its selected metric. */
enum ann_status
ann_vector_from_double(const double *input, uint32_t dimension,
		       enum ann_metric metric, float *out);

/** Validate an already canonical float32 vector. */
enum ann_status
ann_vector_validate_f32(const float *vector, uint32_t dimension,
			enum ann_metric metric);

/** Compute f32_f64_v1 distance, consuming shared control when given. */
enum ann_status
ann_distance(const float *query, const float *vector, uint32_t dimension,
	     enum ann_metric metric, struct ann_search_control *control,
	     double *out);

#if defined(__cplusplus)
} /* extern "C" */
#endif

#endif /* TARANTOOL_LIB_ANN_NUMERIC_H_INCLUDED */
