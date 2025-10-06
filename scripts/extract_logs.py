import os
import tarfile
from datetime import datetime
tar_file = "C:/Users/hemalatha/Desktop/attest-eda/raw_logs/Attest_Archive_2025_Sep_22_10_25_01.tar.gz"
extract_path = "C:/Users/hemalatha/Desktop/attest-eda/data/raw"
with tarfile.open(tar_file,"r:gz") as tar:
    for member in tar.getmembers():
        if member.isfile():
            filename = os.path.basename(member.name)
            date_str = None
            base, _ = os.path.splitext(filename)
            for token in base.split("_"):
                if token.isdigit() and len(token)==8:
                    date_str = token
                    break
            if date_str:
                try:
                    run_date=datetime.strptime(date_str,"%Y%m%d").strftime("%Y-%m-%d")
                except:
                    run_date="unknown_date"
            else:
                run_date="unknown_date"
            parts = base.split("_")
            suite_name = parts[2] if len(parts) > 2 else "unknown_suite"
            target_dir = os.path.join(extract_path,run_date,suite_name)
            os.makedirs(target_dir,exist_ok=True)
            tar.extract(member,target_dir)
print("Logs are extracted:",extract_path)
