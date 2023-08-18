/*

Copyright (C) 2023 Allan Silva

Version 0.1.0, 2023

This software is provided 'as-is', without any express or implied
warranty.  In no event will the author be held liable for any damages
arising from the use of this software.
Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:
1. The origin of this software must not be misrepresented; you must not
   claim that you wrote the original software. If you use this software
   in a product, an acknowledgment in the product documentation would be
   appreciated but is not required.
2. Altered source versions must be plainly marked as such, and must not be
   misrepresented as being the original software.
3. This notice may not be removed or altered from any source distribution.

This code is based on the work of Mark Adler <madler@alumni.caltech.edu>,
Daniela Petruzalek and Pablo Fonseca (https://github.com/eaglebh/blast-dbf).

*/

use jni::objects::{JClass, JObject, JString, JValue};
use jni::JNIEnv;
use libc::{fopen, FILE};
use std::ffi::CString;
use std::fs::metadata;
use std::time::Instant;

struct DecompressStats {
    input_size: i64,
    output_size: i64,
    decompress_time: i64,
    blast_dbf_status_code: i32,
}

#[no_mangle]
pub extern "system" fn Java_br_gov_sus_opendata_dbc_NativeDecompressor_decompress<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input_file: JString<'local>,
    stats: JObject,
) {
    let input_file_name: String = env
        .get_string(&input_file)
        .expect("Can not get inputFile to decompress.")
        .into();
    let output_file_name = input_file_name.clone() + ".dbf";

    match decompress(&input_file_name, &output_file_name) {
        Ok(file_stats) => set_decompress_stats(
            &mut env,
            &stats,
            &input_file_name,
            &output_file_name,
            file_stats,
        ),
        Err(reason) => panic!("{}", reason),
    }
}

#[no_mangle]
pub extern "system" fn Java_br_gov_sus_opendata_dbc_NativeDecompressor_decompressTo<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input_file: JString<'local>,
    output_file: JString<'local>,
    stats: JObject,
) {
    let input_file_name: String = env
        .get_string(&input_file)
        .expect("Can not get inputFile to decompress.")
        .into();
    let output_file_name: String = env
        .get_string(&output_file)
        .expect("Can not get outputFile to decompress.")
        .into();

    match decompress(&input_file_name, &output_file_name) {
        Ok(file_stats) => set_decompress_stats(
            &mut env,
            &stats,
            &input_file_name,
            &output_file_name,
            file_stats,
        ),
        Err(reason) => panic!("{}", reason),
    }
}

fn decompress(input_file: &str, output_file: &str) -> Result<DecompressStats, String> {
    // Input / Output fopen/fwrite C modes.
    let rb_mode = CString::new("rb").expect("Can not create native string to read binary mode.");
    let wb_mode = CString::new("wb").expect("Can not create native string to write binary mode.");

    let c_input_file =
        CString::new(input_file).expect("Can not create native string to input_file.");
    let c_output_file =
        CString::new(output_file).expect("Can not create native string to output_file.");

    let start = Instant::now();

    let status = unsafe {
        let input = fopen(c_input_file.as_ptr(), rb_mode.as_ptr());
        let output = fopen(c_output_file.as_ptr(), wb_mode.as_ptr());
        dbc2dbf(input, output)
    };

    Ok(DecompressStats {
        decompress_time: start.elapsed().as_millis() as i64,
        input_size: metadata(input_file)
            .expect("Can not access metadata of input_file")
            .len() as i64,
        output_size: metadata(output_file)
            .expect("Can not access metadata of output_file")
            .len() as i64,
        blast_dbf_status_code: status,
    })
}

fn set_decompress_stats<'local>(
    env: &mut JNIEnv<'local>,
    stats: &JObject,
    input_file: &str,
    output_file: &str,
    file_stats: DecompressStats,
) {
    // Input file
    let j_input_file = env
        .new_string(&input_file)
        .expect("Can not create input file name for stats.");
    let j_object_input_file = JObject::from(j_input_file);

    env.call_method(
        &stats,
        "setInputFileName",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&j_object_input_file)],
    )
    .expect("Can not set inputFileName.");

    // Output file
    let j_output_file = env
        .new_string(&output_file)
        .expect("Can not create output file name for stats.");
    let j_object_output_file = JObject::from(j_output_file);

    env.call_method(
        &stats,
        "setOutputFileName",
        "(Ljava/lang/String;)V",
        &[JValue::Object(&j_object_output_file)],
    )
    .expect("Can not set outputFileName.");

    // Input file size
    env.set_field(
        &stats,
        "inputFileSize",
        "J",
        JValue::Long(file_stats.input_size),
    )
    .expect("Can not set inputFileSize.");

    // Output file size
    env.set_field(
        &stats,
        "outputFileSize",
        "J",
        JValue::Long(file_stats.output_size),
    )
    .expect("Can not set outputFileSize.");

    // Decompression time
    env.set_field(
        &stats,
        "decompressTime",
        "J",
        JValue::Long(file_stats.decompress_time),
    )
    .expect("Can not set decompressTime.");

    // Decompress status code
    env.set_field(
        &stats,
        "decompressStatusCode",
        "I",
        JValue::Int(file_stats.blast_dbf_status_code),
    )
    .expect("Can not set decompressStatusCode.");
}

#[link(name = "blast-dbf-x86_64.so")]
extern "C" {
    pub fn dbc2dbf(input: *mut FILE, output: *mut FILE) -> i32;
}

#[cfg(test)]
mod tests {
    use super::dbc2dbf;
    use libc::fopen;
    use std::ffi::CString;
    use std::fs::{metadata, remove_file};

    #[test]
    fn exploratory() {
        unsafe {
            let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

            let read_binary_mode = CString::new("rb").unwrap();
            let dbc_file = cargo_manifest_dir + "/resources/test/POBR2023.dbc";

            let write_binary_mode = CString::new("wb").unwrap();
            let dbf_file = dbc_file.clone() + ".dbf";

            let input_file_ptr = CString::new(dbc_file.clone()).unwrap();
            let input = fopen(input_file_ptr.as_ptr(), read_binary_mode.as_ptr());

            let output_file_ptr = CString::new(dbf_file.clone()).unwrap();
            let output = fopen(output_file_ptr.as_ptr(), write_binary_mode.as_ptr());

            let result = dbc2dbf(input, output);
            assert!(result == 0);

            let dbf_len = metadata(&dbf_file).unwrap().len();
            remove_file(dbf_file).unwrap();
            assert!(
                metadata(&dbc_file).unwrap().len() < dbf_len,
                "Uncompressed file is not bigger than compressed file"
            );
        }
    }
}
