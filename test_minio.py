from MinIOUtils import MinIO

m = MinIO()

objs = m.getFileList("wudao")
#print(objs)


#data = m.getFile("wudao", "202212-202301/20230123/wudao.20230123.1.网页/part-2021023489.json")
#print(data)

res = m.putFile("xiao.shuo", "test.txt", "/Users/mc/codes/MinIOUtils/test.txt", tag={"类型": "小说"})
print(res)
