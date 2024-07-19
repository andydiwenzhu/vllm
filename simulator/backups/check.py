with open('11.log') as f:
    lines = f.readlines()
    lines = [line for line in lines if '|INFO|' in line]
    with open('22.log', 'w') as ff:
        ff.writelines(lines)