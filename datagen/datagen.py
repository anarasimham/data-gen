import random
from datetime import datetime

class DataGenerator:
    def __init__(self):
        pass
    def gen_row(self):
        pass

class POSDataGenerator(DataGenerator):
    discounts = [5,10,15,20,25]

    def gen_row(self):
        DataGenerator.gen_row(self)
        row = {}
        row['trxn_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        row['cust_id'] = random.randrange(1101)%1000+1
        row['trxn_amt'] = random.randrange(201)+round(random.random(), 2)
        row['discount_amt'] = self.discounts[random.randrange(5)]
        row['store_id'] = random.randrange(101)
        row['rep_id'] = random.randrange(16)
        row['part_sku'] = chr(random.randint(65,91))+'-'+str(random.randint(800,1001))
        row['qty'] = random.randrange(11)
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
        row['vibr_tolr_pct'] = random.gauss(name_obj['vibr_dist_center'], name_obj['vibr_thrs'])
        row['vibr_tolr_thrs'] = name_obj['vibr_dist_center']-name_obj['vibr_thrs']/4
        row['heat_tolr_pct'] = random.gauss(name_obj['heat_dist_center'], name_obj['heat_thrs']/4)
        row['heat_tolr_thrs'] = name_obj['heat_dist_center']-name_obj['heat_thrs']
        row['qty'] = random.randrange(1,5)
        return row


if __name__ == '__main__':
    d = POSDataGenerator()
    for i in range(1,10):
        print(d.gen_row())
    e = ManufacturingDataGenerator()
    for i in range(1,10):
        print(e.gen_row())
