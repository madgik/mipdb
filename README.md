# mipdb [![codecov](https://codecov.io/gh/madgik/mipdb/branch/main/graph/badge.svg?token=BGF1OU23JA)](https://codecov.io/gh/madgik/mipdb)

mipdb is a tool to validate datasets for the MIP platform.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- **python3.8**
- **pip**

## Installation

You can easily install mipdb using `pip` on python3.8:

```bash
python3.8 -m pip install mipdb
```

## Usage

### Validating a Pathology Folder

**Command**: `mipdb validate-folder <folder_path>`

**Description**:
- The command enforces the following [requirements](https://github.com/HBPMedical/mip-deployment/blob/master/documentation/NewDataRequirements.md).
- First, the metadata file itself is validated.
- Secondly, the csvs in the folder are validated against the metadata.

**Note**: You can nest multiple pathology folders within a parent folder, and the `validate-folder` command will automatically iterate through each pathology folder.

**Examples**:
```bash
  mipdb validate-folder /home/user/data/dementia
```
```bash
  mipdb validate-folder /home/user/data
```