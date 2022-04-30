# from kafka import KafkaConsumer
# consumer = KafkaConsumer('spark')
# for message in consumer:
#     print (message)
from pylab import *
from datetime import datetime
from fpdf import FPDF
import matplotlib.pyplot as plt
import uuid

# make a square figure and axes
plt.figure(1, figsize=(6,6))
plt.axes([0.1, 0.1, 0.8, 0.8])

# The slices will be ordered and plotted counter-clockwise.
labels = 'Frogs', 'Hogs', 'Dogs', 'Logs'
fracs = [15, 30, 45, 10]
explode=(0, 0.05, 0, 0)
id =uuid.uuid4().hex

plt.pie(fracs, explode=explode, labels=labels,
                autopct='%1.1f%%', shadow=True, startangle=90)
                # The default startangle is 0, which would start
                # the Frogs slice on the x-axis.  With startangle=90,
                # everything is rotated counter-clockwise by 90 degrees,
                # so the plotting starts on the positive y-axis.

plt.title('Palavras contabilizadas')
plt.savefig('relatorio-'+id+'.png')

# save FPDF() class into a 
# variable pdf
pdf = FPDF()
  
# Add a page
pdf.add_page()
  
# set style and size of font 
# that you want in the pdf
pdf.set_font("Arial", size = 20)
  
# create a cell
pdf.cell(200, 10, txt = datetime.now().strftime("%d/%m/%Y"), 
         ln = 1, align = 'C')
  
# # add another cell
# pdf.cell(200, 10, txt = "Relatorio com base nas palavras recebidas pelo usuario.",
#          ln = 2, align = 'C')

pdf.image('relatorio-'+id+'.png')
  
# save the pdf with name .pdf
pdf.output('relatorio-'+id+'.pdf')
