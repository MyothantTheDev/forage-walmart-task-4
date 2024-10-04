import csv
import sqlite3

class IDataBase:

  def __init__(self, database): ...

  def connect(self): ...

  def exec(self): ...

  def commit(self): ...

class SqliteDB(IDataBase):

  __db = None
  __dbConnection = None
  __cursor = None

  def __init__(self, database):
    self.__db = database

  def connect(self):
    self.__dbConnection = sqlite3.connect(self.__db)
    self.__cursor = self.__dbConnection.cursor()

  def exec(self, raw_sql, param = None):
    if param == None:
      return self.__cursor.execute(raw_sql)
    if isinstance(param[0], list) and isinstance(param, list):
      return self.__cursor.executemany(raw_sql, param)
    return self.__cursor.execute(raw_sql, param)
  
  def commit(self):
    self.__dbConnection.commit()
    
class SQL:

  SELECT = "SELECT"
  ALL = "*"
  INSERT_INTO = "INSERT INTO"
  VALUES = "VALUES"
  FROM = "FROM"
  WHERE = "WHERE"
  AND = "AND"

  def insertQuery(self, columns, values):
    table = self.__class__.__name__
    columns = ", ".join(columns)
    value = "({});".format(", ".join(f"?" for _ in values))
    query = f"{self.INSERT_INTO} {table}({columns}) {self.VALUES} {value}"
    return query
  
  def getOneQuery(self, columns, **condition):
    if len(condition) == 0: raise ValueError("No conditions provided for the query.")

    condition_statements = []
    operator = condition.pop('operator', None)

    if operator != None:
      #for multipal operator
      if isinstance(operator, list):
        condition_items = list(condition.items())
        for i, (table_column, condition_value) in enumerate(condition_items):
          condition_statements.append(f"{table_column} = {self.datatype_check(condition_value)}")
          if i < len(operator):
            condition_statements.append(operator[i])
        condition_statement = f" ".join(condition_statements)
      else:
        # for one operator
        for table_column, condition_value in condition.items():
          condition_statements.append(f"{table_column} = {self.datatype_check(condition_value)}")
        condition_statement = f" {operator} ".join(condition_statements)
    else:
      #for one condition pair and there is no operator contain
      table_column, condition_value = condition.popitem()
      condition_statement = f"{table_column} = {self.datatype_check(condition_value)}"

    table = self.__class__.__name__
    columns = ", ".join(columns)
    query = f"{self.SELECT} {columns} {self.FROM} {table} {self.WHERE} {condition_statement};"
    return query
  
  def datatype_check(self, value):
    if isinstance(value, str):
      return f"\'{value}\'"
    return f"{value}"

class Config:

  db_instance = None

  @classmethod
  def DBconfig(cls, type, database):
    type = type.lower()
    match type:
      case "sqlite":
        cls.db_instance = SqliteDB(database)
      case "psql":
        pass
      case "mysql":
        pass
      case _:
        raise Exception("type must be stirng and database type.")
      
  @classmethod
  def getDBInstance(cls) -> SqliteDB:
    return cls.db_instance
  
class Model(SQL):

  def __init__(self, **value):
    self.mix_values = value
    self.db_instance = Config.getDBInstance()
    self.db_instance.connect()

  def getInstance(self) -> list: # Get class instances. In this function, it get columns instance
    for attribute in self.__class__.__dict__.keys():
      if attribute[:2] != "__":
        return getattr(self.__class__, attribute)
    return None
  
  def getValues(self):
    valueList = []
    columns = self.getInstance()
    if columns == None:
      raise Exception('Missing Columns.')
    for column in columns:
      valueList.append(self.mix_values.get(column))
    return valueList
  
  def save(self):
    value = tuple(self.getValues())
    raw_sql = self.insertQuery(self.getInstance(), value)
    self.db_instance.exec(raw_sql, value)
    self.db_instance.commit()

  def getOne(self, **condition):
    raw_sql = self.getOneQuery(self.getInstance(), **condition)
    # print(raw_sql)
    exec_sql = self.db_instance.exec(raw_sql)
    return exec_sql.fetchone()

class Product(Model):
  column = ['id', 'name']

class Shipment(Model):
  column = ['id', 'product_id', 'quantity', 'origin', 'destination']

def csv_data_generator(iterObject):
  for item in iterObject:
    yield item.values()

def csv_reader(file):
  file = open(f'./data/{file}')
  return csv.DictReader(file)

def main():
  Config.DBconfig('sqlite','shipment_database.db')
  raw_data_shipping_0 = csv_data_generator(csv_reader("shipping_data_0.csv"))
  product_primary = 0
  has_data = True
  shipment_id = 0

  while has_data:
    try:
      origin_warehouse,destination_store,product_name,_,product_quantity,_ = next(raw_data_shipping_0)
      get_product = Product().getOne(name=product_name)

      shipment_id += 1

      if get_product: 
        product_id, _ = get_product
        shipment_table = Shipment(id=shipment_id, product_id=product_id, quantity=product_quantity, origin=origin_warehouse, destination=destination_store)
        shipment_table.save()
      else:
        product_primary += 1
        product_table =Product(id=product_primary, name=product_name)
        shipment_table=Shipment(id=shipment_id, product_id=product_primary, quantity=product_quantity, origin=origin_warehouse, destination=destination_store)
        product_table.save()
        shipment_table.save()
    except StopIteration:
      has_data = False

  raw_data_shipping_1 = csv_reader("shipping_data_1.csv")
  raw_data_shipping_2 = csv_reader("shipping_data_2.csv")

  shipment_location = {}
  shipment_items = {}
  has_data = True

  for row in raw_data_shipping_2:
    shipment_identifier = row['shipment_identifier']
    shipment_location[shipment_identifier] = (row['origin_warehouse'],row['destination_store'])

  for row in raw_data_shipping_1:
    shipment_identifier = row["shipment_identifier"]
    shipment_item = shipment_items.get(shipment_identifier)
    if shipment_item != None:
      product_quantity = shipment_item.get(row["product"])
      if product_quantity == None:
        shipment_item[row["product"]] = 1
      else:
        product_quantity += 1
        shipment_item[row["product"]] = product_quantity
    else:
      product_name = row["product"]
      shipment_items[shipment_identifier] = {product_name: 1}

  for identifier in shipment_items.keys():
    origin_warehouse, destination_store = shipment_location[identifier]
    products = shipment_items[identifier]
    for product_name, product_quantity in products.items():
      shipment_id += 1
      product = Product().getOne(name=product_name)
      if product != None:
        product_id, _ = product
        shipment_table = Shipment(id=shipment_id, product_id=product_id, quantity=product_quantity, origin=origin_warehouse, destination=destination_store)
        shipment_table.save()
      else:
        product_primary += 1
        product_table = Product(id=product_primary, name=product_name)
        shipment_table = Shipment(id=shipment_id, product_id=product_primary, quantity=product_quantity, origin=origin_warehouse, destination=destination_store)
        product_table.save()
        shipment_table.save()
  print(product_primary)

if __name__ == "__main__":
  main()