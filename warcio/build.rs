use std::env;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use ini::Ini;
use uncased::UncasedStr;

fn main() {
    let base_path: PathBuf = env::var("OUT_DIR").unwrap().into();
    let ini_dir = "data/";
    println!("cargo:rerun-if-changed={}", ini_dir);

    for entry in std::fs::read_dir(ini_dir).expect("failed to read data dir") {
        let entry = entry.expect("error reading directory entry");
        let ini_path = entry.path();
        // Only try to read *.ini
        if ini_path.extension().map(|ext| ext != "ini").unwrap_or(true) {
            continue;
        }

        let ini = Ini::load_from_file(&ini_path).unwrap();
        generate_conversions(&ini, &base_path).expect(&format!(
            "Failed to generate conversions from {:?}",
            ini_path
        ));
    }
}

fn generate_conversions(ini: &Ini, base_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let out_path = base_path.to_owned().join(
        ini.general_section()
            .get("file")
            .expect("file property missing from ini general section"),
    );
    let type_name = ini
        .general_section()
        .get("type")
        .expect("type property missing from ini general section");
    let catchall_variant = ini
        .general_section()
        .get("catchall")
        .expect("catchall property missing from ini general section");

    let mut out = BufWriter::new(File::create(&out_path).unwrap());
    let mut map = phf_codegen::Map::<&'static UncasedStr>::new();

    writeln!(
        &mut out,
        "impl std::convert::AsRef<str> for {t} {{
    fn as_ref(&self) -> &str {{
        use {t}::*;
        match self {{",
        t = type_name
    )?;

    for (variant, repr) in ini
        .section(Some("values"))
        .expect("values section missing from ini")
        .iter()
    {
        writeln!(&mut out, "            {} => \"{}\",", variant, repr)?;

        map.entry(
            UncasedStr::new(repr),
            &format!("{}::{}", type_name, variant),
        );
    }

    writeln!(
        &mut out,
        "            {}(s) => s,
        }}
    }}
}}\n",
        catchall_variant
    )?;

    writeln!(
        &mut out,
        "impl<S: AsRef<str> + Into<String>> std::convert::From<S> for {t} {{
    fn from(s: S) -> Self {{
        use uncased::UncasedStr;
        static MAP: phf::Map<&'static UncasedStr, {t}> = {m};

        MAP
            .get(UncasedStr::new(s.as_ref()))
            .map(Clone::clone)
            .unwrap_or({t}::{catchall}(s.into().into_boxed_str()))
    }}
}}\n",
        t = type_name,
        catchall = catchall_variant,
        m = map.build()
    )?;
    Ok(())
}
