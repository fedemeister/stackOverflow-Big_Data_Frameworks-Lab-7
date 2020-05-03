rawTextRDD = sc.textFile("/FileStore/tables/shortStackOverflow.txt")

def isSolved(answerId):
    if len(answerId) > 0:
        return True
    else:
        return False


def convert(line):
    fields = line.split(',')
    if int(fields[0]) == 1:
        return int(fields[1]), (1, 0, isSolved(fields[2]), fields[5])
    else:
        return int(fields[3]), (0, 1)

questionsAndAnswersRDD = rawTextRDD.map(convert)

def sumaVeces(a, b):
    if a[0] == 0 and b[0] == 0:
        return 0, a[1] + b[1]
    elif a[0] == 1:
        return 1, a[1] + b[1], a[2], a[3]
    else:
        return 1, a[1] + b[1], b[2], b[3]
  
sumadoRDD = questionsAndAnswersRDD.reduceByKey(sumaVeces)


def function(pair):
    return (pair[1][3], pair[1][2]), pair[1][1]

tagRDD = sumadoRDD.map(function)


reducidoTagRDD = tagRDD.combineByKey(
    (lambda x: (x, 1)),                        # createCombiner
    (lambda x, y: (x[0] + y, x[1] + 1)),       # mergeValue
    (lambda x, y: (x[0] + y[0], x[1] + y[1]))  # mergeCombiners
)

def extraccion(pair):
    clave = pair[0]
    valor = pair[1]
    tag = clave[0]
    issolved = clave[1]
    n_times = valor[0]
    n_questions = valor[1]
    if not issolved:
        return tag, (0, 'NaN', n_questions, n_times / n_questions)
    else:
        return tag, (n_questions, n_times / n_questions, 0, 'NaN')

estadisticasRDD = reducidoTagRDD.map(extraccion)

def reduccion(a, b):
    if a[1] == 'NaN':
        return b[0], b[1], a[2], a[3]
    else:
        return a[0], a[1], b[2], b[3]

RBK_RDD = estadisticasRDD.reduceByKey(reduccion)

RBK_RDD.sortBy(lambda a: a[1],False).collect()

#Out[166]: [('Java', (3, 2.0, 1, 0.0)), ('PHP', (2, 1.5, 0, 'NaN')), ('Python', (1, 3.0, 1, 2.0)), ('C#', (0, 'NaN', 2, 3.5))]