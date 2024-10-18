import pandas as pd

class FlatFileWriter:
    def __init__(self, file_path, invalid_records_path):
        self.file_path = file_path
        self.invalid_records_path = invalid_records_path
        self.file_pointer = None
        self.invalid_records = None  # Renamed log_pointer to invalid_records
        # Define the layout mapping with columns appearing in multiple places
        self.layouts = {
            'r1': {
                'id': [(0, 10)],  # Place 'id' in one position
                'name': [(10, 30), (40, 60)],  # Place 'name' in two places
                'profile_info': [(30, 40)]  # Single position for 'profile_info'
            },
            'r2': {
                'id': [(0, 10)], 
                'order_id': [(10, 20)], 
                'order_details': [(20, 50)]
            },
            'r3': {
                'id': [(0, 10)], 
                'address': [(10, 40)], 
                'city': [(40, 50)]
            },
            'r4': {
                'id': [(0, 10)], 
                'phone': [(10, 20)], 
                'email': [(20, 50)]
            },
        }

    def open_files(self):
        self.file_pointer = open(self.file_path, 'a')
        self.invalid_records = open(self.invalid_records_path, 'a')  # Open the invalid records log

    def close_files(self):
        if self.file_pointer:
            self.file_pointer.close()
        if self.invalid_records:
            self.invalid_records.close()

    def write_flat_file(self, dataframes_dict):
        # Find the unique ids by taking all the unique 'id' values across all dataframes
        all_ids = pd.concat([df['id'] for df in dataframes_dict.values()]).unique()

        # For each id, check if it exists in all dataframes
        for person_id in all_ids:
            missing_from = []
            present_in = []

            for df_type, df in dataframes_dict.items():
                if person_id not in df['id'].values:
                    missing_from.append(df_type)
                else:
                    present_in.append(df_type)

            if missing_from:
                # Log to the invalid_records if the ID is missing from some dataframes
                self.invalid_records.write(
                    f"ID {person_id} is missing from: {', '.join(missing_from)}. Present in: {', '.join(present_in)}\n"
                )
            else:
                # Process the ID and write to the flat file if present in all dataframes
                for df_type, df in dataframes_dict.items():
                    layout = self.layouts[df_type]
                    # Filter the dataframe to get the rows for the current person_id
                    df_filtered = df[df['id'] == person_id]
                    for _, row in df_filtered.iterrows():
                        fixed_length_row = [' '] * 50  # Assume 50 is the max length of the line
                        for col, positions in layout.items():
                            value = str(row[col])
                            # Place the same column value in multiple positions
                            for (start, end) in positions:
                                fixed_length_row[start:end] = value.ljust(end - start)[:end - start]
                        # Write the fixed-length row to the file
                        self.file_pointer.write(''.join(fixed_length_row) + '\n')

    # New start_process method within the same class
    def start_process(self, dataframes_dict):
        # Open files globally
        self.open_files()

        # Write flat file using the provided dataframes
        self.write_flat_file(dataframes_dict)

        # Close the files after writing is complete
        self.close_files()

# Example usage from another class or file
def main():
    # Example data for two people
    r1_data = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 
               'profile_info': ['InfoA', 'InfoB', 'InfoC']}
    r2_data = {'id': [1, 2], 'order_id': [100, 200], 'order_details': ['DetailsA', 'DetailsB']}
    r3_data = {'id': [1], 'address': ['123 St'], 'city': ['NY']}
    r4_data = {'id': [2], 'phone': ['0987654321'], 'email': ['bob@test.com']}

    r1_df = pd.DataFrame(r1_data)
    r2_df = pd.DataFrame(r2_data)
    r3_df = pd.DataFrame(r3_data)
    r4_df = pd.DataFrame(r4_data)

    dataframes_dict = {
        'r1': r1_df,
        'r2': r2_df,
        'r3': r3_df,
        'r4': r4_df
    }

    # Create an instance of FlatFileWriter and call start_process
    writer = FlatFileWriter('output_file', 'invalid_records_file')
    writer.start_process(dataframes_dict)

if __name__ == "__main__":
    main()
