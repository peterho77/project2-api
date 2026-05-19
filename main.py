matrix = [[1,3,5,7],[10,11,16,20],[23,30,34,60]]
m,n = len(matrix), len(matrix[0])
arr = [matrix[i][j] for i in range(m) for j in range(n)]
print(arr)