#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create MySQL schema file

# import the MySQLdb and sys modules
import MySQLdb
import sys

### Script to generate information about paperdata schema
### Opens MySQL through module, creates file by iterating through list query generates

### Author: Immanuel Washington
### Date: 10-07-14

class TableFormatter:

    @staticmethod
    def MAX(x,y):
        return x if x > y else y

    def __init__(self, column_names=['column_1', 'column_2']):
        self.rowData = []
        self.dataAlignType = '<'
        self.headerAlignType = '<'
        self.column_names = column_names
        self.columns = len(column_names)
        self.column_size = len(max(column_names))
        self.__setAlignment = lambda x: '>' if x == 2 else ('^' if x == 1 else '<')
        self.dataPadding = True
        self.padDisplayValue = '-'
        self.dataRowSeparation = True

    def setDataRowSeparation(self, separate=True):
        self.dataRowSeparation = separate
    
    def setPaddingString(self, padString):
        self.padDisplayValue = padString
        
    def setDataPadding(self, padding):
        """Allow padding of columns when data row has fewer elements than the maximum columns. This option is set by default"""
        self.dataPadding = True if padding else False
        
    def setDataAlignment(self, align = 0):
        """Set the text alignment, LEFT by default. Valid values are 0:LEFT 1:CENTER 2:RIGHT"""
        self.dataAlignType = self.__setAlignment(align % 3)

    def setHeaderAlignment(self, align = 0):
        """Set the text alignment, LEFT by default. Valid values are 0:LEFT 1:CENTER 2:RIGHT"""
        self.headerAlignType = self.__setAlignment(align % 3)
        
    def addDataColumn(self, data_row):
        data_size = len(data_row)
        if (data_size > self.columns) or (data_size < self.columns and not self.dataPadding):
            raise Exception('Requires {0} columns, found {1} as {2}'.
                            format(self.columns, len(data_row), data_row))
        elif self.dataPadding and data_size < self.columns:
            self.rowData.append(data_row + [None] * (self.columns - data_size))
        else:
            self.rowData.append(data_row)
        #self.column_size= self.MAX(len(max(data_row)), self.column_size)
	#self.column_size = self.column_size 
        self.column_size = len(max(data_row))
	
    def addDataColumns(self, data_rows):
        for data_row in data_rows:
            data_size = len(data_row)
            if (data_size > self.columns) or (data_size < self.columns and not self.dataPadding):
                raise Exception('Requires {0} columns, found {1} as {2}'.
                                format(self.columns, len(data_row), data_row))
            elif self.dataPadding and data_size < self.columns:
                self.rowData.append(list(data_row) + [None] * (self.columns - data_size))
            else:
                self.rowData.append(data_row)
            #self.column_size= self.MAX(len(max(data_row)), self.column_size)
            self.column_size = len(max(data_row))
            #self.column_size = self.column_size 

    def __str__(self):
        return self.createTable()
    
    def createTable(self):
        headerFormat = '{0:' + self.headerAlignType + '{width}}|'
        dataFormat = '{0:' + self.dataAlignType + '{width}}|'
        colhead = '-' * self.column_size
        header = '+'
        out = '|'
        for a in self.column_names:
            out = out + headerFormat.format(a, width=self.column_size)
            header = header + '{0}+'.format(colhead)        
        out = header + '\n' + out + '\n' + header + '\n'
        for v in self.rowData:
            if self.dataRowSeparation:
                out = out + header + '\n'
            out = out + '|'
            for a in v:
                strVal = self.padDisplayValue[:self.column_size]
                if a != None:
                    if type(a).__name__ == 'bool':
                        strVal = repr(a)
                    else:
                        strVal = a
                out = out + dataFormat.format(strVal, width=self.column_size)
            out = out + '\n' ### should be optional + header + '\n'
        out = out + header
        return out
  
if __name__ == "__main__":
    tf = TableFormatter(["Era", "Julian Day", "Raw Path", "Compressed Path", "Amount"])

    connection = MySQLdb.connect (host = 'shredder', user = 'paperboy', passwd = 'paperboy', db = 'paperdata', local_infile=True)
    cursor = connection.cursor()

    cursor.execute('''SELECT era, julian_day, SUBSTRING_INDEX(raw_location, 'z', 1), SUBSTRING_INDEX(path, 'z', 1), count(*) from paperdata group by julian_day, SUBSTRING_INDEX(raw_location, 'z', 1), SUBSTRING_INDEX(path, 'z', 1) order by julian_day ASC''')
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    #tf.setDataPadding(True)
    tf.setDataAlignment(0)
    tf.setHeaderAlignment(1)
    tf.setDataRowSeparation(False)
    #tf.setPaddingString('*')
    tf.addDataColumns(results)
    #for item in results:
    #    tf.addDataColumn(item)
    #tf.addDataColumn(['val1', 'value2', 'value 3'])
    #print tf.createTable()
    #print tf

    with open('/data2/home/immwa/scripts/paper/table_descr.txt', 'w') as df:
        df.write(tf.createTable())
