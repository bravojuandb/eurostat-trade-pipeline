import os
from pathlib import Path
import subprocess


"""

Python runs comext_download.sh
    │
    ├── Bash runs fake curl
    │       └── fake curl exits with 18
    │
    ├── Bash records curl_exit=18
    ├── Bash takes the failure branch and removes the partial file
    └── Bash executes exit 1
            │
            └── Python receives result.returncode == 1

"""

def test_bash_download_fails(tmp_path):
    project_root = Path(__file__).resolve().parents[2]
    script_path = project_root / "src/bronze/comext_download.sh"
    fake_bin = project_root / "tests/bronze/fixtures"

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["DRY_RUN"] = "0"

    result = subprocess.run(
        ["bash", str(script_path), "2003-01", "2003-01"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    print(f"1: {result.returncode}")
    print(f"2: {result.stdout}")
    print(f"3: {result.stderr}")
    print(f"4: {result.args}")

    final_file = tmp_path / "data/raw/comext_products/2003-01/full_v2_200301.7z"
    part_file = tmp_path / "data/raw/comext_products/2003-01/full_v2_200301.7z.part"

    assert not final_file.exists()
    assert not part_file.exists()

    assert result.returncode == 1
    assert "curl_exit=18" in result.stdout

