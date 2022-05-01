from pylab import *
from datetime import datetime
from fpdf import FPDF
import matplotlib.pyplot as plt
import uuid
# from kafka import KafkaConsumer


# consumer = KafkaConsumer('spark')
# for message in consumer:
#     print (message)

plt.figure(1, figsize=(6,6))
plt.axes([0.1, 0.1, 0.8, 0.8])

labels = 'Frogs', 'Hogs', 'Dogs', 'Logs'
fracs = [15, 30, 45, 10]
explode=(0, 0.05, 0, 0)
id =uuid.uuid4().hex

plt.pie(
    fracs, explode=explode, labels=labels,
    autopct='%1.1f%%', shadow=True, startangle=90
)

plt.title('Palavras contabilizadas')
plt.savefig('relatorio-'+id+'.png')

pdf = FPDF()
  
pdf.add_page()

pdf.set_font("Arial", size = 20)
  
# create a cell
pdf.cell(200, 10, txt = datetime.now().strftime("%d/%m/%Y"), 
         ln = 1, align = 'C')
  
pdf.image('relatorio-'+id+'.png')
  
pdf.output('relatorio-'+id+'.pdf')
