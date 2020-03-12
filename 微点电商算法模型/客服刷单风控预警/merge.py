import pandas as pd  
import os  
SaveFile_Name = r'behaviorlog.csv'              #合并后要保存的文件名  
#将该文件夹下的所有文件名存入一个列表  
url='./csv'
file_list = os.listdir(url)  
  
#读取第一个CSV文件并包含表头  
df = pd.read_csv(url+'/'+file_list[0])
#将读取的第一个CSV文件写入合并后的文件保存  
df.to_csv(SaveFile_Name,encoding="utf_8_sig",index=False)  
  
#循环遍历列表中各个CSV文件名，并追加到合并后的文件  
for i in range(1,len(file_list)):  
    df = pd.read_csv(url+'/'+file_list[i])
    df.to_csv(SaveFile_Name,encoding="utf_8_sig",index=False, header=False, mode='a+')