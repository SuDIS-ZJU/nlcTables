import random

def split_data(input_file, train_file, dev_file, test_file, dev_ratio=0.2, test_ratio=0.2):
    # 读取所有行数据
    with open(input_file, 'r') as f:
        lines = f.readlines()

    # 打乱数据顺序
    random.shuffle(lines)

    # 计算发展集和测试集的大小
    dev_size = int(len(lines) * dev_ratio)
    test_size = int(len(lines) * test_ratio)

    # 分割数据集
    dev_data = lines[:dev_size]
    test_data = lines[dev_size:dev_size + test_size]
    train_data = lines[dev_size + test_size:]

    # 写入发展集
    with open(dev_file, 'w') as f:
        f.writelines(dev_data)

    # 写入测试集
    with open(test_file, 'w') as f:
        f.writelines(test_data)

    # 写入训练集
    with open(train_file, 'w') as f:
        f.writelines(train_data)

# 使用示例
input_file = 'all-2575u2/queries-test.txt'  # 替换为实际输入文件路径
train_file = 'all-2575u2/train_query.txt'
dev_file = 'all-2575u2/dev_query.txt'
test_file = 'all-2575u2/test_query.txt'

split_data(input_file, train_file, dev_file, test_file)