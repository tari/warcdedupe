use std::env;

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

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

        let out_path = base_path.to_owned().join(
            ini.general_section()
                .get("file")
                .expect("file property missing from ini general section"),
        );
        let type_name = ini
            .general_section()
            .get("type")
            .expect("type property missing from ini general section");

        generate_conversions(
            BufWriter::new(File::create(&out_path).expect("failed to create output file")),
            type_name,
            ini.section(Some("values"))
                .expect("values section missing from ini")
                .iter(),
        )
        .unwrap_or_else(|_| panic!("Failed to generate conversions from {:?}", ini_path));
    }
}

fn generate_conversions<'a, W: Write, V: Iterator<Item = (&'a str, &'a str)>>(
    mut out: W,
    type_name: &str,
    variants: V,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut map = phf_codegen::Map::<&'static UncasedStr>::new();

    writeln!(
        &mut out,
        "impl std::convert::AsRef<str> for {t} {{
    fn as_ref(&self) -> &str {{
        use {t}::*;
        match self {{",
        t = type_name
    )?;

    for (variant, repr) in variants {
        writeln!(&mut out, "            {} => \"{}\",", variant, repr)?;

        map.entry(
            UncasedStr::new(repr),
            &format!("{}::{}", type_name, variant),
        );
    }

    writeln!(
        &mut out,
        "        }}
    }}
}}\n"
    )?;

    writeln!(
        &mut out,
        "impl std::convert::TryFrom<&str> for {t} {{
    /// Conversion from string fails if the string is not a known kind.
    type Error = ();

    fn try_from(s: &str) -> Result<Self, ()> {{
        use uncased::AsUncased;
        static MAP: phf::Map<&'static ::uncased::UncasedStr, {t}> = {m};

        MAP
            .get(s.as_uncased())
            // Just a copy since get() returns a ref
            .map(Clone::clone)
            .ok_or(())
    }}
}}\n",
        t = type_name,
        m = map.build(),
    )?;
    Ok(())
}
