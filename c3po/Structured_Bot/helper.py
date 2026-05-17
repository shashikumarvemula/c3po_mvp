import re

class FileNameExtractor:
    """
    A static class to extract file names from Python code related to saved images, Excel files, and PowerPoint presentations.
    """
    image_files = []
    excel_files = []
    ppt_files = []
    
    @staticmethod
    def extract_file_names(python_code, regex_pattern, file_type):
        """
        Extracts file names based on the given regex pattern.

        Args:
            python_code (str): The Python code to search for file names.
            regex_pattern (str): The regex pattern to use for extracting file names.
            file_type (str): The type of file being extracted.

        Returns:
            list: A list of file names found in the code.
        """
        matches = re.findall(regex_pattern, str(python_code))
        file_names_lst = list(set(matches))  # Remove duplicates if any

        if file_names_lst:
            for i, file_name in enumerate(file_names_lst):
                print(f"Saved {file_type} name {i+1}: {file_name}")
        else:
            print(f"No {file_type} names found.")

        return file_names_lst

    @classmethod
    def extract_images_names(cls, python_code):
        """Extract image file names."""
        file_type = "image"
        cls.image_files = cls.extract_file_names(python_code, r"plt\.savefig\('(.*?)'", file_type)
        return cls.image_files  # Ensure the list is returned
    
    @classmethod
    def extract_pkl_names(cls, python_code):
        """Extract PKL file names from user-specific directory."""
        print("Extracting PKL file names...")
        print("Python code for PKL extraction:", python_code)
        file_type = "PKL"
        
        # Pattern 1: pickle.dump with open() - regular string
        pickle_dump_old = cls.extract_file_names(
            python_code, 
            r"pickle\.dump\([^,]+,\s*open\(['\"]([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        # Pattern 2: pickle.dump with open() - f-string
        pickle_dump_new = cls.extract_file_names(
            python_code, 
            r"pickle\.dump\([^,]+,\s*open\(f['\"](?:{user_dir}/)?([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        # Pattern 3: with open() - regular string
        with_open_old = cls.extract_file_names(
            python_code, 
            r"with\s+open\(['\"]([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        # Pattern 4: with open() - f-string
        with_open_new = cls.extract_file_names(
            python_code, 
            r"with\s+open\(f['\"](?:{user_dir}/)?([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        # Pattern 5: Variable assignment (filename = "something.pkl")
        variable_assignment = cls.extract_file_names(
            python_code,
            r"filename\s*=\s*['\"]([^'\"]+\.pkl)['\"]",
            file_type
        )
        
        # Pattern 6: Any variable assignment ending with .pkl
        any_assignment = cls.extract_file_names(
            python_code,
            r"\w+\s*=\s*['\"]([^'\"]+\.pkl)['\"]",
            file_type
        )
        
        # Pattern 7: Direct .dump() method
        direct_save_old = cls.extract_file_names(
            python_code, 
            r"\.dump\(['\"]([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        # Pattern 8: Direct .dump() method - f-string
        direct_save_new = cls.extract_file_names(
            python_code, 
            r"\.dump\(f['\"](?:{user_dir}/)?([^'\"]+\.pkl)['\"]", 
            file_type
        )
        
        cls.pkl_files = (pickle_dump_old + pickle_dump_new + with_open_old + 
                        with_open_new + variable_assignment + any_assignment + 
                        direct_save_old + direct_save_new)
        
        return list(set(cls.pkl_files))  # Remove duplicates
    
    @classmethod
    def extract_excel_names(cls, python_code):
        """Extract Excel/CSV file names."""
        file_type = "Excel/CSV"
        
        files = []
        
        # Pattern 1: Standard method calls with regular strings
        pattern1 = r"(?:wb\.save|to_excel|to_csv|\.to_excel|\.to_csv)\(['\"]([^'\"]+\.(?:xlsx?|csv))['\"]"
        files_1 = cls.extract_file_names(python_code, pattern1, file_type)
        files.extend(files_1)
        
        # Pattern 2: Method calls with f-strings
        pattern2 = r"(?:wb\.save|to_excel|to_csv|\.to_excel|\.to_csv)\(f['\"]([^'\"]+\.(?:xlsx?|csv))['\"]"
        files_2 = cls.extract_file_names(python_code, pattern2, file_type)
        files.extend(files_2)
        
        # Pattern 3: Variable assignment for CSV/Excel files
        pattern3 = r"\w+\s*=\s*['\"]([^'\"]+\.(?:xlsx?|csv))['\"]"
        files_3 = cls.extract_file_names(python_code, pattern3, file_type)
        files.extend(files_3)
        
        # Pattern 4: pd.DataFrame.to_csv or result.to_csv with variable
        pattern4 = r"\.to_csv\(([a-zA-Z_]\w*)[,\)]"
        files_4 = cls.extract_file_names(python_code, pattern4, file_type)
        files.extend(files_4)
        
        # Pattern 5: pd.DataFrame.to_excel or result.to_excel with variable
        pattern5 = r"\.to_excel\(([a-zA-Z_]\w*)[,\)]"
        files_5 = cls.extract_file_names(python_code, pattern5, file_type)
        files.extend(files_5)
        
        # Pattern 6: writer.save() or ExcelWriter patterns
        pattern6 = r"pd\.ExcelWriter\(['\"]([^'\"]+\.xlsx?)['\"]"
        files_6 = cls.extract_file_names(python_code, pattern6, file_type)
        files.extend(files_6)
        
        # Pattern 7: csv.writer or open() with .csv
        pattern7 = r"open\(['\"]([^'\"]+\.csv)['\"]"
        files_7 = cls.extract_file_names(python_code, pattern7, file_type)
        files.extend(files_7)
        
        # Pattern 8: open() with f-string for CSV
        pattern8 = r"open\(f['\"]([^'\"]+\.csv)['\"]"
        files_8 = cls.extract_file_names(python_code, pattern8, file_type)
        files.extend(files_8)
        
        cls.excel_files = list(set(files))
        return cls.excel_files
            


    @classmethod
    def extract_ppt_names(cls, python_code):
        """Extract PowerPoint file names."""
        file_type = "PPT"
        regex_ppt = r"prs\.save\('(.*?)'|['\"]([^'\"]+\.pptx)['\"]"
        cls.ppt_files = cls.extract_file_names(python_code, regex_ppt, file_type)
        return cls.ppt_files  # Ensure the list is returned