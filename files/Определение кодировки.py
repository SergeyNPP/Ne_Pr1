import chardet

# Read the text file
with open("D:/DE/VSCODE/AIRFLOW/test/product_info_csv.csv", 'rb') as f:
    data = f.read()

# Detect the encoding
result = chardet.detect(data)
print(result['encoding'])

# В командную строку cmd
# C:\Users\buano>python d:/DE/VSCODE/AIRFLOW/test/1.py
# windows-1251