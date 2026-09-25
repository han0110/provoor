#ifndef ERE_VERIFIER_H
#define ERE_VERIFIER_H

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Operation succeeded.
 */
#define ERE_OK 0

/**
 * A required pointer argument was null when a value was expected.
 */
#define ERE_ERR_NULL_PTR 1

/**
 * `zkvm_kind` was not one of the documented values.
 */
#define ERE_ERR_BAD_KIND 2

/**
 * The program verifying key bytes failed to decode.
 */
#define ERE_ERR_DECODE_PROGRAM_VK 3

/**
 * The proof bytes failed to decode.
 */
#define ERE_ERR_DECODE_PROOF 4

/**
 * The proof was well-formed but failed cryptographic verification.
 */
#define ERE_ERR_VERIFY 5

/**
 * An unexpected internal condition occurred. This indicates a bug in the
 * binding or the verifier library rather than an invalid argument.
 */
#define ERE_ERR_INTERNAL 6

/**
 * Opaque handle to a verifier bound to a program verifying key. Created by
 * [`ere_verifier_new`] and released by [`ere_verifier_free`].
 */
typedef struct EreVerifier EreVerifier;

/**
 * Constructs a verifier bound to an encoded program verifying key for the
 * selected zkVM.
 *
 * `zkvm_kind` selects the target zkVM.
 *
 * - `0` - [`zkVMKind::OpenVM`]
 * - `1` - [`zkVMKind::SP1`]
 * - `2` - [`zkVMKind::Zisk`]
 *
 * On success, writes the new handle into `*output` and returns [`ERE_OK`]. The
 * caller owns the handle and must release it with [`ere_verifier_free`]. On
 * error, `*output` is set to null and the corresponding status code is
 * returned.
 *
 * # Safety
 *
 * - `encoded_program_vk_ptr` must point to `encoded_program_vk_len` readable bytes (or be null
 *   when `encoded_program_vk_len == 0`).
 * - `output` must be a non-null, writable `*mut *mut EreVerifier`.
 */
int32_t ere_verifier_new(uint32_t zkvm_kind,
                         const uint8_t *encoded_program_vk_ptr,
                         uintptr_t encoded_program_vk_len,
                         struct EreVerifier **output);

/**
 * Verifies a proof against the verifier's program verifying key and allocates a
 * buffer holding the proven public values.
 *
 * On success, stores the buffer pointer into `*public_values_ptr` and its
 * length into `*public_values_len`, and returns [`ERE_OK`]. The caller owns the
 * buffer and must release it with [`ere_bytes_free`], passing back the exact
 * `(pointer, length)` pair. Empty public values are reported as a null pointer
 * and zero length. On error, both out-parameters are cleared to null and zero
 * and the corresponding status code is returned.
 *
 * # Safety
 *
 * - `handle` must be a live handle returned by [`ere_verifier_new`].
 * - `encoded_proof_ptr` must point to `encoded_proof_len` readable bytes (or be null when
 *   `encoded_proof_len == 0`).
 * - `public_values_ptr` must be a non-null, writable `*mut *mut u8`.
 * - `public_values_len` must be a non-null, writable `*mut usize`.
 */
int32_t ere_verifier_verify(const struct EreVerifier *handle,
                            const uint8_t *encoded_proof_ptr,
                            uintptr_t encoded_proof_len,
                            uint8_t **public_values_ptr,
                            uintptr_t *public_values_len);

/**
 * Writes the zkVM kind the verifier was constructed for into `*output` and
 * returns [`ERE_OK`].
 *
 * # Safety
 *
 * - `handle` must be a live handle returned by [`ere_verifier_new`].
 * - `output` must be a non-null, writable `*mut u32`.
 */
int32_t ere_verifier_zkvm_kind(const struct EreVerifier *handle, uint32_t *output);

/**
 * Releases a verifier handle. The handle must not be used after this call.
 * Passing a null pointer is a no-op.
 *
 * # Safety
 *
 * `handle` must be a live handle returned by [`ere_verifier_new`] or null.
 */
void ere_verifier_free(struct EreVerifier *handle);

/**
 * Releases a byte buffer that this library allocated and handed back through a
 * `(pointer, length)` pair. The pair must be exactly what the allocating call
 * produced, and the buffer must not be used after this call. A null pointer or
 * zero length is a no-op.
 *
 * # Safety
 *
 * `ptr` and `len` must be an unmodified pair previously produced by a function
 * in this library that documents [`ere_bytes_free`] as its release routine and
 * not already freed, or `ptr` must be null.
 */
void ere_bytes_free(uint8_t *ptr, uintptr_t len);

#endif  /* ERE_VERIFIER_H */
