import os
import asyncio
import pandas as pd

def find_project_root(project_dir : str) -> str:
    """
    Walks up the directory tree from the current working directory until it finds the directory named project_dir.
    Returns the absolute path of the project root directory.
    Raises FileNotFoundError if not found.
    """
    current_path = os.path.abspath(os.getcwd())
    while True:
        if os.path.basename(current_path) == project_dir:
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:  # Reached root of filesystem
            break
        current_path = parent_path
    raise FileNotFoundError(f"Project root directory '{project_dir}' not found from current directory upward.")



def collect_checklists_data(checklist_data_path :str = "data/checklists") -> pd.DataFrame:
    # checklist_data_path = "../data/checklists"
    project_root = find_project_root(os.getenv('PROJECT_ROOT_DIR'))
    absolute_dir = os.path.join(project_root, checklist_data_path)
    checklist_files_list = os.listdir(absolute_dir)
    checklist_df = pd.DataFrame()
    for file in checklist_files_list:
        if file.endswith('.tsv'):
            file_path = os.path.join(absolute_dir, file)
            checklist_df = pd.concat([checklist_df, pd.read_csv(file_path, sep="\t")], ignore_index=True)

    return checklist_df


async def collect_checklists_data_async(checklist_data_path: str = "data/checklists") -> pd.DataFrame:
    """Asynchronously load and concatenate all TSV checklist files into a single DataFrame.

    Uses asyncio.to_thread to parallelize blocking pandas.read_csv calls without extra dependencies.
    """
    project_root = find_project_root(os.getenv('PROJECT_ROOT_DIR'))
    absolute_dir = os.path.join(project_root, checklist_data_path)
    if not os.path.isdir(absolute_dir):
        raise FileNotFoundError(f"Checklist directory not found: {absolute_dir}")

    tsv_files = [
        os.path.join(absolute_dir, file)
        for file in os.listdir(absolute_dir)
        if file.endswith('.tsv')
    ]

    if not tsv_files:
        return pd.DataFrame()

    # Read all TSVs concurrently using threads (I/O-bound)
    read_tasks = [asyncio.to_thread(pd.read_csv, file_path, sep="\t") for file_path in tsv_files]
    dataframes = await asyncio.gather(*read_tasks)

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()