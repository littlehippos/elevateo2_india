# Inventory.py
# This script is to sanitize data sets obtained from India on equipment inventories.
# This script processes xls sheets specific to a single state in India.

import csv, sqlite3

# These map the Excel field names to field names in the resulting database
INVENTORY_FIELDS = [{'field_name': 'district', 'excel_name': 'DISTRICT', 'type': 'TEXT'},
                    {'field_name': 'facility', 'excel_name': 'Facility Type', 'type': 'TEXT'},
                    {'field_name': 'facility_name', 'excel_name': 'Name of Facility','type': 'TEXT'},
                    {'field_name': 'equipment', 'excel_name': 'Equipment name','type': 'TEXT'},
                    {'field_name': 'location', 'excel_name': 'Equipment category','type': 'TEXT'},
                    {'field_name': 'manufacturer', 'excel_name': 'Manufacturer','type': 'TEXT'},
                    {'field_name': 'amc', 'excel_name': 'AMC / CMC (Y=1,N=0)','type': 'TEXT'},
                    {'field_name': 'status', 'excel_name': 'Working status W-WORKING/NW- NOT WORKING/NI-NOT INSTALLED/PACKED', 'type': 'TEXT'}
                    ]

# These are used to define the schema for the facilityinfo table in the resulting database
FACILITYINFO_FIELDS = [{'field_name': 'district', 'type': 'TEXT'},
                       {'field_name': 'facility_name', 'type': 'TEXT'},
                       {'field_name': 'facility', 'type': 'TEXT'},
                       {'field_name': 'location', 'type': 'TEXT'},
                       {'field_name': 'coordinated_use', 'type': 'INT'},
                       {'field_name': 'oxygen_concentrators', 'type': 'INT'},
                       {'field_name': 'oxygen_cylinders', 'type': 'INT'},
                       {'field_name': 'pulse_oximeters', 'type': 'INT'}]

# These are used to define the schema for the stateinfo table in the resulting database
STATEINFO_FIELDS = [{'field_name': 'Total_facilities', 'type': 'INT'},
                    {'field_name': 'DH', 'type': 'INT'},
                    {'field_name': 'CHC', 'type': 'INT'},
                    {'field_name': 'PHC', 'type': 'INT'},
                    {'field_name': 'Other_facility_types', 'type': 'INT'},
                    {'field_name': 'Oxygen_concentrators', 'type': 'INT'},
                    {'field_name': 'Oxygen_cylinders', 'type': 'INT'},
                    {'field_name': 'Pulse_oximeters', 'type': 'INT'},
                    {'field_name': 'Percent_facilities_with_coordinated_use', 'type': 'INT'},
                    {'field_name': 'DH_with_coordinated_use', 'type': 'INT'},
                    {'field_name': 'CHC_with_coordinated_use', 'type': 'INT'},
                    {'field_name': 'PHC_with_coordinated_use', 'type': 'INT'},
                    {'field_name': 'Other_facilities_with_coordinated_use', 'type': 'INT'}
                    ]

# Defines the various types of oxygen equipment found in the Excel sheets
O2_EQUIP = ['oxygen concentrator', 'oxygen cylinder', 'oxygen humidifier', 'pulse oximeter']

TERM_KEY = [{'field_name': 'equipment', 'term': 'oxygen cylinder', 'alt': 'oxygen cyclinder'},
            {'field_name': 'location', 'term': 'store room', 'alt': 'store'},
            {'field_name': 'location', 'term': 'surgical', 'alt': 'surgical (ot)'},
            {'field_name': 'location', 'term': 'surgical', 'alt': 'surgery'},
            {'field_name': 'location', 'term': 'surgical', 'alt': 'surgical ward'},
            {'field_name': 'location', 'term': 'emergency', 'alt': 'emergency ward'},
            {'field_name': 'location', 'term': 'other', 'alt': 'others'},
            {'field_name': 'location', 'term': 'sncu', 'alt': 'sncu store room'},
            {'field_name': 'location', 'term': 'ot', 'alt': 'o t'},
            {'field_name': 'location', 'term': 'ot', 'alt': 'ot-general'},
            {'field_name': 'location', 'term': 'ot store', 'alt': 'o.t.store'},
            {'field_name': 'location', 'term': 'opd', 'alt': 'opd  '},
            {'field_name': 'location', 'term': 'opd', 'alt': 'opd room'},
            {'field_name': 'location', 'term': 'dental', 'alt': 'dentel'},
            {'field_name': 'location', 'term': 'emergency', 'alt': 'emergency ward'},
            {'field_name': 'location', 'term': 'eye ot', 'alt': 'eye'},
            {'field_name': 'location', 'term': 'sterilization', 'alt': 'sterlization'},
            {'field_name': 'location', 'term': 'eye ot', 'alt': 'eye store'},
            {'field_name': 'location', 'term': 'maternity', 'alt': 'meternity'},
            {'field_name': 'location', 'term': 'maternity', 'alt': 'meternity room'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'maternal & child care'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'w&c helth'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'maternal and child care'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'm & child care'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'maternal and child'},
            {'field_name': 'location', 'term': 'mch', 'alt': 'maternal and child health'},
            {'field_name': 'location', 'term': 'female ward', 'alt': 'female ward room'},
            {'field_name': 'location', 'term': 'icu', 'alt': 'icu room'},
            {'field_name': 'location', 'term': 'icu', 'alt': 'i c u'},
            {'field_name': 'location', 'term': 'laboratory', 'alt': 'lab'},
            {'field_name': 'location', 'term': 'labour room', 'alt': 'labour ward'},
            {'field_name': 'location', 'term': 'child health', 'alt': 'child ward'},
            {'field_name': 'location', 'term': 'vip', 'alt': 'vip room'}
            ]

def create_inventory_table(conn):
        cur = conn.cursor()
        a = ", ".join(['{} {}'.format(k['field_name'], k['type']) for k in INVENTORY_FIELDS])
        cur.execute('CREATE TABLE Inventory({})'.format(a))
        conn.commit()
        print 'Created table: Inventory'
        print

def create_facilityinfo_table(conn):
    cur = conn.cursor()
    a = ", ".join(['{} {}'.format(k['field_name'], k['type']) for k in FACILITYINFO_FIELDS])
    cur.execute('CREATE TABLE Facility_info ({})'.format(a))
    conn.commit()
    print 'Created table: Facility_info '
    print

def create_stateinfo_table(conn):
    cur = conn.cursor()
    a = ", ".join(['{} {}'.format(k['field_name'], k['type']) for k in STATEINFO_FIELDS])
    cur.execute('CREATE TABLE State_info({})'.format(a))
    conn.commit()
    print 'Created table: State_info'
    print

def clear_table(conn, table):  # type(table) = str
    cur = conn.cursor()
    cur.execute('DROP TABLE {}'.format(table))
    conn.commit()
    print "Cleared table: ", table
    print


def table_exists(conn, table):
    cur = conn.cursor()
    cur.execute('SELECT * FROM sqlite_master WHERE type="table" AND name="{}"'.format(table))
    return bool(cur.fetchall())

def preview_table(conn, table):
    # Prints first 3 rows of database, displays # of entries, and # districts
    cur = conn.cursor()
    cur.execute('SELECT * FROM {} LIMIT 3'.format(table))
    text = cur.fetchall()
    conn.commit()
    print 'Preview of {} (top 3 rows): '.format(table)
    for item in text: print item
    print

    entries = total_entries(conn, table)
    districts = total_districts(conn, table)
    facilities = total_facilities(conn)
    print '...Total entries: ', entries
    print '...Total districts: ', districts
    print '...Total facilities: ', facilities
    print
    return (districts, entries, facilities)

def clean_table(conn):
    cur = conn.cursor()

    print "Inventory cleaned: "
    for item in TERM_KEY:
        cur.execute('UPDATE Inventory SET {field_name} = "{term}" WHERE {field_name} = "{alt}"'.format(**item))
        print "'{alt}' <-- {term}".format(**item)
    conn.commit()
    print

def total_entries(conn, table):
    cur = conn.cursor()
    cur.execute('SELECT COUNT (*) FROM {}'.format(table))
    total = cur.fetchone()[0]
    print 'Total entries in {}: {}'.format(table, total)
    print
    return total

def total_facilities(conn):
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT district, facility, facility_name FROM Inventory')
    total = len(cur.fetchall())
    print 'Total facilities in state: ', total
    print
    return total

def total_rooms(conn):
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT district, facility, facility_name, location FROM Inventory')
    total = len(cur.fetchall())
    print 'Total rooms in state: ', total
    print
    return total

def populate_inventory(conn, state_csv):
    cur = conn.cursor()
    excel_keys = [field_info['excel_name'] for field_info in INVENTORY_FIELDS]
    db_keys = [field_info['field_name'] for field_info in INVENTORY_FIELDS]
    db_keys = ", ".join(db_keys)
    num_keys = ",".join('?'* len(excel_keys))

    with open(state_csv, 'rb') as csvfile:
        records = csv.DictReader(csvfile)
        for i, record in enumerate(records):
            if 'oxygen' in record['Equipment name'].lower():
                to_db = [record[excel_key].lower() for excel_key in excel_keys]
                cur.execute('INSERT INTO Inventory({})'
                            'VALUES ({})'.format(db_keys, num_keys),to_db)
            if 'pulse' in record['Equipment name'].lower():
                to_db = [record[excel_key].lower() for excel_key in excel_keys]
                cur.execute('INSERT INTO Inventory ({})'
                            'VALUES ({})'.format(db_keys, num_keys),to_db)
    conn.commit()
    print 'Populated table: Inventory'
    print


def populate_facility_info(conn): # returns dict of data
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT district, facility_name, facility, location, equipment, '
                'COUNT (*) FROM Inventory '
                'GROUP BY district, facility_name, location, equipment')
    ans = cur.fetchall()
    facility_keys = [(item[0:4]) for item in ans]  # Create key of facility info
    equip_values = [(item[4:6]) for item in ans]  # Create values of equipment info
    facility_info = zip(facility_keys, equip_values) # facility_keys, equipment_values

    error = set()
    new_db = {}
    for k, v in facility_info:  # Tally oxygen-equipment per facility
        if k not in new_db:
            new_db[k] = {u'oxygen concentrator': 0, u'oxygen cylinder': 0, u'pulse oximeter': 0, 'coordinated use': 0}
        if v[0] in new_db[k].keys(): new_db[k][v[0]] += v[1]
        else: error.add(v[0])

    for k, v in new_db.items():
        v['coordinated use'] = bool((v[u'oxygen concentrator'] or v[u'oxygen cylinder']) and v[u'pulse oximeter'])

    db_keys = [field_info['field_name'] for field_info in FACILITYINFO_FIELDS]
    num_keys = ",".join('?' * len(db_keys))
    db_keys = ", ".join(db_keys)

    for k, v in new_db.items():
        to_db_k = ['coordinated use', 'oxygen concentrator', 'oxygen cylinder', 'pulse oximeter']
        to_db = list(k + tuple([v[k] for k in to_db_k]))
        cur.execute('INSERT INTO Facility_info ({})'
                    'VALUES ({})'.format(db_keys, num_keys), to_db)
    conn.commit()
    # Turn to_db to dict
    print "Populated table: Facility_info"
    print "Table fields: ", db_keys
    print "Equipment not added to Facility_info: ", ", ".join(error) # errors: oxygen humidifier and oxygen cyclinder
    print

    return new_db

def populate_stateinfo(conn): # return dict
    cur = conn.cursor()

    db_keys = [field_info['field_name'] for field_info in STATEINFO_FIELDS]
    db_keys = ", ".join(db_keys)
    num_keys = ",".join('?' * len(db_keys))

    state_info = {}
    state_info.update(state_equip_breakdown(conn)) #gives total, oxygen concentrator, pulse ox, oxygen cylinder, and humidifier
    state_info.update(state_facility_breakdown(conn)) # gives total, phc, dh, chc, other
    state_info.update(coordinated_use_breakdown(conn)) # gives total, phc, dh, chc, other, coordinated use

def state_equip_breakdown(conn):
    cur = conn.cursor()
    cur.execute('SELECT equipment, COUNT (*) from Inventory GROUP BY equipment')
    breakdown = cur.fetchall()
    total = sum([item[1] for item in breakdown])

    print 'Breakdown of oxygen-related equipment: '
    for item in breakdown: print ("{}s: {}".format(*item))
    print "Total: {}".format(total)
    print

    breakdown = dict(breakdown)
    return breakdown

def state_facility_breakdown(conn, display=0):
    cur = conn.cursor()
    cur.execute('SELECT a.facility, COUNT (*) FROM '
                '(SELECT DISTINCT facility_name, facility FROM Inventory) a '
                'GROUP BY a.facility')
    conn.commit()
    breakdown = cur.fetchall()
    total = total_facilities(conn)

    if display == 1:
        print 'Breakdown of facility types: '
        for item in breakdown: print ("{}s: {}".format(*item))  # can i do this if state_info were a dict?
        print "Total: {}".format(total)
        print

    breakdown = dict(breakdown)
    to_del = [k for k in breakdown.keys() if (k not in [u'dh', u'chc', u'phc'])]
    total_other = sum([breakdown[k] for k in to_del])
    breakdown.update({'total': total})
    breakdown.update({'other': total_other})
    for k in to_del: del breakdown[k]

    return breakdown


def coordinated_use_breakdown(conn, display=0):
    cur = conn.cursor()
    cur.execute('SELECT facility, COUNT (*) FROM Facility_info WHERE coordinated_use > 0 GROUP BY facility')
    breakdown = cur.fetchall()
    total = sum([item[1] for item in breakdown])

    if display==1:
        print 'Breakdown of coordinated use by facility types: '
        for item in breakdown: print ("{}: {}".format(*item))
        print 'Total: {}'.format(total)
        print

    all_facilities = total_facilities(conn)
    coordinated_use_ratio = float(total)/all_facilities
    print '% Facilities with coordinated use: ', round(coordinated_use_ratio * 100, 1),'%'
    print

    breakdown = dict(breakdown)
    to_del = [k for k in breakdown.keys() if (k not in [u'dh', u'chc', u'phc'])]
    total_other = sum([breakdown[k] for k in to_del])
    breakdown.update({'total': total})
    breakdown.update({'other': total_other})
    breakdown.update({'% Facilities with coordinated use': coordinated_use_ratio})
    for k in to_del: del breakdown[k]

    return breakdown

def coordinated_use_room_breakdown(conn):
    cur = conn.cursor()
    cur.execute('SELECT location, COUNT (*) FROM Facility_info WHERE coordinated_use > 0 GROUP BY location')
    breakdown = cur.fetchall()
    total = sum([item[1] for item in breakdown])
    print 'Breakdown of coordinated use by room: '
    for item in breakdown: print ("{}: {}".format(*item))
    print 'Total rooms: {}'.format(total)
    print

    all_rooms = total_rooms(conn)
    coordinated_use_ratio_rooms = float(total) / all_rooms
    print '% Rooms with coordinated use: ', round(coordinated_use_ratio_rooms * 100, 1), '%'
    print

    breakdown = dict(breakdown)
    breakdown.update({'% Rooms with coordinated use': coordinated_use_ratio_rooms})
    return breakdown

def total_districts(conn, table):
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT district FROM {}'.format(table))
    total = len(cur.fetchall())  # 2158
    conn.commit()
    print 'The number of total districts: ', total
    print
    return total

def oxygen_equip_location_breakdown(conn, equip, display = 0): # type(equip) = str & in O2_EQUIP
    cur = conn.cursor()
    if equip is 'all':
        equip = 'Oxygen-related equipment'
        cur.execute('SELECT DISTINCT district, facility_name, facility, location, COUNT (*) '
                    'FROM Inventory '
                    'GROUP BY location')
    elif equip not in O2_EQUIP: print "Error. Must input string from ", O2_EQUIP
    else:
        cur.execute('SELECT DISTINCT district, facility_name, facility, location, COUNT (*) '
                    'FROM Inventory '
                    'WHERE equipment = "{}" '
                    'GROUP BY location'.format(equip))

    locations = cur.fetchall()

    if display == 1:
        print "{}S are used in the following locations: ".format(equip.upper())
        for i in locations: print i[3], i[4]
        print

    return locations

####################################
# Create connection
conn = sqlite3.connect('Inventory.db')
cur = conn.cursor()

# # Set up and clean 'Inventory.db'
# Table: Inventory
if table_exists(conn, 'Inventory'): # Remove when done
    clear_table(conn, 'Inventory')

state_csv = 'file'
create_inventory_table(conn)
populate_inventory(conn, state_csv)
clean_table(conn)
districts, entries, facilities = preview_table(conn, 'Inventory') # type(districts) = type(entries) = int

# Table: Facility_info
if table_exists(conn, 'Facility_info'): # Remove when done
    clear_table(conn, 'Facility_info')

create_facilityinfo_table(conn)
facility_db = populate_facility_info(conn)

# Table: State_info
if table_exists(conn, 'State_info'):  # Remove when done
    clear_table(conn, 'State_info')

create_stateinfo_table(conn)
state_db = populate_stateinfo(conn)
