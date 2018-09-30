import random
from datetime import datetime
from faker import Faker

rep_count = 16

class DataGeneratorFactory(object):
    def factory(type_str):
        if type_str == 'customer':
            return CustomerDataGenerator()
        elif type_str == 'part_dashboard':
            return ManufacturingDataGenerator()
        elif type_str == 'transactions':
            return POSDataGenerator()
        elif type_str == 'transactions_customer':
            return POSCustomerDataGenerator()
        elif type_str == 'rep':
            return SalesRepDataGenerator()
    factory = staticmethod(factory)

class DataGenerator(object):
    def __init__(self):
        pass
    def gen_row(self):
        pass

class CustomerDataGenerator(DataGenerator):
    fake = None
    def __init__(self):
        self.fake = Faker()

    def gen_row(self):
        row = {}
        row['cust_contact_name'] = self.fake.name()
        row['cust_ssn'] = self.fake.ssn().replace('-','')
        row['cust_date_reg'] = str(self.fake
            .date_this_decade(before_today=True, after_today=False))
        row['cust_is_active'] = 1 if random.random() > .2 else 0
        row['cust_address'] = self.fake.address().replace('\n',', ')
        row['cust_company_name'] = self.fake.company()
        return row

class SalesRepDataGenerator(DataGenerator):
    fake = None
    reps_left = rep_count

    def __init__(self):
        self.fake = Faker()

    def gen_row(self):
        row = {}
        row['id'] = self.reps_left
        self.reps_left -= 1

        if self.reps_left == -1:
            return None

        row['name'] = self.fake.name()
        row['address'] = self.fake.address().replace('\n',', ')
        row['salary'] = random.randrange(5,11)*10000
        row['quota'] = random.randrange(5,11)*100000
        row['start_date'] = str(self.fake
            .date_this_decade(before_today=True, after_today=False))

        return row

class POSDataGenerator(DataGenerator):
    discounts = [5,10,15,20,25]

    def gen_row(self):
        row = {}
        row['trxn_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row['cust_id'] = random.randrange(1101)%1000+1
        row['trxn_amt'] = random.randrange(201)+round(random.random(), 2)
        row['discount_amt'] = self.discounts[random.randrange(5)]
        row['store_id'] = random.randrange(101)
        row['rep_id'] = random.randrange(rep_count)
        row['part_sku'] = chr(random.randint(65,91))+'-'+str(random.randint(800,1001))
        row['qty'] = random.randrange(11)
        return row

class POSCustomerDataGenerator(POSDataGenerator):
    cust_gen = CustomerDataGenerator()

    def gen_row(self):
        row = self.cust_gen.gen_row()
        row.update(super(POSCustomerDataGenerator, self).gen_row())
        row.pop('cust_id')
        return row


class ManufacturingDataGenerator(DataGenerator):
    name_to_notes = [
            {'name':'rearbumper','notes':'Rear bumper, bent on edges'},
            {'name':'frontleftdoor','notes':'driver door'},
            {'name':'frontrightdoor','notes':'passenger door'},
            {'name':'backleftdoor','notes':'rear driver side door'},
            {'name':'backrightdoor','notes':'rear passenger side door'},
            {'name':'hood','notes':'covering for the engine'},
            {'name':'controlarm','notes':'support for wheel'},
            {'name':'headgasket','notes':'sensitive part, needs extensive testing'},
            {'name':'piston','notes':'dynamic part'},
            {'name':'exhaustpipe','notes':'need to test additionally for corrosion from exhaust'},
            {'name':'catalyticconverter','notes':'compliance requirements'}
            ]

    def __init__(self):
        for item in self.name_to_notes:
            item['vibr_dist_center'] = random.uniform(.75, .95)
            item['heat_dist_center'] = random.uniform(.75, .95)
            item['vibr_thrs'] = random.uniform(0.1,0.2)
            item['heat_thrs'] = random.uniform(0.05,0.15)

    def gen_row(self):
        DataGenerator.gen_row(self)
        row = {}

        name_obj = self.name_to_notes[random.randrange(len(self.name_to_notes))]
        row['shortname'] = name_obj['name']
        row['time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row['notes'] = name_obj['notes']
        row['part_loc'] = random.randrange(1,10)
        row['vibr_tolr_pct'] = random.gauss(name_obj['vibr_dist_center'], name_obj['vibr_thrs']*1.5)
        row['vibr_tolr_thrs'] = name_obj['vibr_dist_center']-name_obj['vibr_thrs']
        row['heat_tolr_pct'] = random.gauss(name_obj['heat_dist_center'], name_obj['heat_thrs']*1.5)
        row['heat_tolr_thrs'] = name_obj['heat_dist_center']-name_obj['heat_thrs']
        row['qty'] = random.randrange(1,5)
        return row


if __name__ == '__main__':
    #d = POSDataGenerator()
    #for i in range(1,10):
    #    print(d.gen_row())
    #e = ManufacturingDataGenerator()
    #for i in range(1,10):
    #    print(e.gen_row())
    #c = CustomerDataGenerator()
    #for i in range(1,10):
    #    print(c.gen_row())
    p = POSCustomerDataGenerator()
    for i in range(1,10):
        print(p.gen_row())
