
### MIPDB Installation and Usage Guide

**mipdb** is a tool to validate datasets for the MIP platform.

#### Prerequisites

Before you begin, ensure you have met the following requirements:
- Python 3.8
- pip

### Installation Steps

1. **Update Package List:**
   ```bash
   sudo apt update
   ```

2. **Install Python 3.8:**
   ```bash
   sudo apt install python3.8
   ```

3. **Verify Python 3.8 Installation:**
   ```bash
   python3.8 --version
   ```

4. **Install pip for Python 3.8:**
   ```bash
   sudo apt install python3-pip
   ```

5. **Verify pip Installation:**
   ```bash
   python3.8 -m pip --version
   ```

6. **Install mipdb Using pip:**
   ```bash
   python3.8 -m pip install mipdb
   ```

### Setting Up PATH

7. **Update PATH to Include Local Binary Directory:**

   Run this command to dynamically fetch the user base path and update the PATH environment variable:
   ```bash
   export PATH="$PATH:$(python3.8 -m site --user-base)/bin"
   ```

   To make this change permanent, add this line to your `~/.bashrc` or `~/.profile` file:
   ```bash
   echo 'export PATH="$PATH:$(python3.8 -m site --user-base)/bin"' >> ~/.bashrc
   source ~/.bashrc
   ```
   or
   ```bash
   echo 'export PATH="$PATH:$(python3.8 -m site --user-base)/bin"' >> ~/.profile
   source ~/.profile
   ```

### Usage

#### Validating a Pathology Folder

**Command:**
```bash
mipdb validate-folder <folder_path>
```

**Description:**
- The command enforces the following requirements:
  - First, the metadata file itself is validated.
  - Secondly, the CSV files in the folder are validated against the metadata.
- You can nest multiple pathology folders within a parent folder, and the `validate-folder` command will automatically iterate through each pathology folder.

* The folder has to comply with these requirements:
https://github.com/HBPMedical/mip-deployment/blob/master/doc/NewDataRequirements.md

**Examples:**
```bash
mipdb validate-folder /home/user/data/dementia
mipdb validate-folder /home/user/data
```
