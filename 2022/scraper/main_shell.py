import subprocess as sp
# type_list = ["sdat", "tax"]
if __name__ == '__main__':
    print(f"main script started for multi_main")
    sp.run(['python', 'multi_main.py'])
    print("\n\n\n\n")
    print(f"main script started for aa tax")
    sp.run(["python","aa_tax_main.py"])
    print("\n\n\n\n")
    print(f"main script started for mnt tax")
    sp.run(["python","mnt_tax_main.py"])
    print("\n\n\n\n")
    print(f"main script started for pg tax")
    sp.run(["python","pg_tax_main.py"])
    print("\n\n\n\n")
    print(f"running script of final.py")
    sp.run(["python", "final.py"])
    print("\n\n\n\n")
    print(f"running script of excel_formating.py")
    sp.run(['python', "excel_formating.py"])






    # for j in range(1,11):
    # print(f"main script started for aa tax {j} 0")
    # sp.run(["python3","aa_tax_main.py"])
    # print(f"main script started for mnt tax {j} 0")
    # sp.run(["python3","mnt_tax_main.py"])
    # print(f"main script started for pg tax {j} 0")
    # sp.run(["python3","pg_tax_main.py"])
            
    # sp.run(["python3","aa_tax_main.py"])
    # sp.run(["python3","mnt_tax_main.py"])
    # sp.run(["python3","pg_tax_main.py"])
    print("Data Scraping process is completed....")
    

    

