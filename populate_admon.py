#!/usr/bin/python3
# -*- coding: iso-8859-15 -*-

import logging, os, re, sys


def path_data_file(datadir=os.path.dirname(__file__) or os.getcwd(), name=''):
    return os.path.join(datadir, 'data', name)


# https://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python#1019572
def _make_gen(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def rawgencount(filename):
    with open(filename, 'rb') as f:
        f_gen = _make_gen(f.raw.read)
        return sum(buf.count(b'\n') for buf in f_gen)


def populate(uri, datadir=os.path.dirname(__file__) or os.getcwd()):

    from proteus import config, Model

    import csv

    # http://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
    csv.field_size_limit(sys.maxsize)

    from decimal import Decimal
    from datetime import datetime
    from tqdm import tqdm

    config = config.set_trytond(uri)

    dbname = os.environ['DB_NAME']

    # config = config.set_trytond('sqlite://')
    # config = config.set_trytond(config_file='/etc/tryton/trytond.conf', database='admon', user='admin')
    # config = config.set_xmlrpc('https://user:passwd@ip:port/databasename')

    from sql import Table

    # must be after config.set_trytond call or will look always for sqlite database
    from trytond.transaction import Transaction

    AccountConfiguration = Model.get('bank.configuration-account')
    Address = Model.get('party.address')
    AddressFormat = Model.get('party.address.format')
    Bank = Model.get('bank')
    BankAccount = Model.get('bank.account')
    BankAccountNumber = Model.get('bank.account.number')
    BankConfiguration = Model.get('bank.configuration-bank')
    Category = Model.get('party.category')
    Company = Model.get('company.company')
    CompanyConfiguration = Model.get('company.configuration')
    CondoFactor = Model.get('condo.factor')
    CondoPain = Model.get('condo.payment.pain')
    CondoParty = Model.get('condo.party')
    CondoPayment = Model.get('condo.payment')
    CondoPaymentGroup = Model.get('condo.payment.group')
    CondoPaymentGroupConfiguration = Model.get('condo.payment.group.configuration')
    CondoUnit = Model.get('condo.unit')
    ContactMechanism = Model.get('party.contact_mechanism')
    Country = Model.get('country.country')
    Currency = Model.get('currency.currency')
    Group = Model.get('res.group')
    Holidays = Model.get('holidays.calendar')
    HolidaysEvent = Model.get('holidays.event')
    HolidaysEventRRule = Model.get('holidays.event.rrule')
    Identifier = Model.get('party.identifier')
    Lang = Model.get('ir.lang')
    Mandate = Model.get('condo.payment.sepa.mandate')
    MandateConfiguration = Model.get('condo.payment.sepa.mandate.configuration')
    ModelData = Model.get('ir.model.data')
    Party = Model.get('party.party')
    PartyConfiguration = Model.get('party.configuration')
    PartyRelation = Model.get('party.relation.all')
    PartyRelationType = Model.get('party.relation.type')
    Recurrence = Model.get('recurrence')
    RecurrenceDate = Model.get('recurrence.date')
    RecurrenceEvent = Model.get('recurrence.event')
    Sequence = Model.get('ir.sequence')
    Subdivision = Model.get('country.subdivision')
    Translation = Model.get('ir.translation')
    UnitFactor = Model.get('condo.unit-factor')
    User = Model.get('res.user')
    ViewSearch = Model.get('ir.ui.view_search')

    table = {}
    idaccount = {}
    idaddress = {}
    idcompany = {}
    idmandate = {}
    idparties = {}
    idpains = {}
    idunits = {}
    idhlds = {}
    iduser = {}

    cache_currency = {}
    cache_country = {}
    cache_lang = {}
    cache_subdivision = {}

    ni = 0
    nt = 11

    def desc(n: int) -> str:
        return 'Load {0:<' + str(27 - len(str(n))) + 's} ({1}/{2})'

    party_seq, = Sequence.find([('name', '=', 'Party')])
    party_seq.number_next = 10001
    party_seq.save()

    pgnull = r'\N'

    def get_bankaccountnumber(old_id):

        if old_id == pgnull:
            logging.warning('<function get_bankaccountnumber>: Function called with null id')
            return None

        new_accountnumber = None

        with open(path_data_file(datadir, 'bank_account_number.csv'), 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter='\t')
            accountnumber = next(filter(lambda f: f['id'] == old_id, csvreader), None)

        if accountnumber:
            new_accountnumbers = BankAccountNumber.find([('number', '=', accountnumber['number'])])
            if new_accountnumbers and len(new_accountnumbers) == 1:
                new_accountnumber = new_accountnumbers[0]
            else:
                logging.error(
                    '<function get_bankaccountnumber>: Bank account number not found: ' + accountnumber['number']
                )
        else:
            logging.error('<function get_bankaccountnumber>: Bank account number not found id: ' + old_id)

        return new_accountnumber

    def get_company(old_id):

        company = Company(idcompany[old_id])

        if not company:
            logging.error('<function get_company>: Company not found id: ' + old_id)

        return company

    def get_country(old_id):

        if old_id == pgnull:
            logging.warning('<function get_country>: Function called with null id')
            return None

        new_country = None

        if old_id in cache_country:
            return Country(cache_country[old_id])

        with open(path_data_file(datadir, 'country_country.csv'), 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter='\t')
            country = next(filter(lambda f: f['id'] == old_id, csvreader), None)

        if country:
            new_countries = Country.find([('code', '=', country['code'])])
            if new_countries and len(new_countries) == 1:
                new_country = new_countries[0]
                cache_country[old_id] = new_country.id
            else:
                logging.error('<function get_country>: Country not found code: ' + country['code'])
        else:
            logging.error('<function get_country>: Country not found id: ' + old_id)

        return new_country

    def get_currency(old_id):

        if old_id == pgnull:
            logging.warning('<function get_currency>: Function called with null id')
            return None

        new_currency = None

        if old_id in cache_currency:
            return Currency(cache_currency[old_id])

        with open(path_data_file(datadir, 'currency_currency.csv'), 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter='\t')
            currency = next(filter(lambda f: f['id'] == old_id, csvreader), None)

        if currency:
            new_currencies = Currency.find([('code', '=', currency['code'])])
            if new_currencies and len(new_currencies) == 1:
                new_currency = new_currencies[0]
                cache_currency[old_id] = new_currency.id
            else:
                logging.error('<function get_currency>: Currency not found code: ' + currency['code'])
        else:
            logging.error('<function get_currency>: Currency not found id: ' + old_id)

        return new_currency

    def get_lang(old_id):

        if old_id == pgnull:
            logging.warning('<function get_lang>: Function called with null id')
            return None

        new_lang = None

        if old_id in cache_lang:
            return Lang(cache_lang[old_id])

        with open(path_data_file(datadir, 'ir_lang.csv'), 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter='\t')
            lang = next(filter(lambda f: f['id'] == old_id, csvreader), None)

        if lang:
            new_langs = Lang.find([('code', '=', lang['code'])])
            if new_langs and len(new_langs) == 1:
                new_lang = new_langs[0]
                cache_lang[old_id] = new_lang.id
            else:
                logging.error('<function get_lang>: Lang not found code: ' + lang['code'])
        else:
            logging.error('<function get_lang>: Lang not found id: ' + old_id)

        return new_lang

    def get_party(old_id):

        party = Party(idparties[old_id])

        if not party:
            logging.error('<function get_party>: Party not found id: ' + old_id)

        return party

    def get_subdivision(old_id, country):

        if old_id in cache_subdivision:
            return Subdivision(cache_subdivision[old_id])

        with open(path_data_file(datadir, 'country_subdivision.csv'), 'r') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter='\t')
            subdivision = next(filter(lambda f: f['id'] == old_id, csvreader), None)

        if subdivision and country:
            new_subdivision, = Subdivision.find([('code', '=', subdivision['code']), ('country', '=', country.id)])

        if not new_subdivision:
            logging.error('<function get_subdivision>: Subdivision not found id: ' + old_id)
        else:
            cache_subdivision[old_id] = new_subdivision.id

        return new_subdivision

    def set_translation(row, table, field):

        with open(path_data_file(datadir, 'ir_translation.csv'), 'r') as _csvfile:
            _csvreader = csv.DictReader(_csvfile, delimiter='\t')

            for _row in filter(
                lambda f: f['name'] == (table + ',' + field) and f['type'] == 'model' and f['res_id'] == row['id'],
                _csvreader,
            ):

                if _row['src'] != row[field]:
                    logging.warning(
                        '<ir_translation>: src of translation "{0}" not equal to {1} {2} "{3}"'.format(
                            _row['src'], table, field, row[field]
                        )
                    )

                translations = Translation.find(
                    [
                        ('lang', '=', _row['lang']),
                        ('name', '=', table + ',' + field),
                        ('res_id', '=', row['_new_id']),
                        ('src', '=', row[field]),
                        ('type', '=', 'model'),
                    ]
                )

                if not len(translations):
                    _record = Translation(
                        lang=_row['lang'],
                        module=_row['module'] if _row['module'] != pgnull else None,
                        name=_row['name'] if _row['name'] != pgnull else None,
                        res_id=int(row['_new_id']),
                        src=row[field],  # _row['src'] should be equal to row[field]
                        type=_row['type'] if _row['type'] != pgnull else None,
                        value=_row['value'],
                    )
                    _record.save()

    # Begin default values
    record = PartyConfiguration(1)
    _save = False

    with open(path_data_file(datadir, 'party_configuration_party_country.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            country = get_country(row['party_country'])

            if country:
                _save = True
                record.party_country = country

    # get only a general value (not company specific)
    with open(path_data_file(datadir, 'party_configuration_party_lang.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in filter(lambda f: f['company'] == pgnull, csvreader):
            lang = get_lang(row['party_lang'])

            if lang:
                _save = True
                record.party_lang = lang

    with open(path_data_file(datadir, 'party_configuration_party_phonecountry.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            country = get_country(row['party_phonecountry'])

            if country:
                _save = True
                record.party_phonecountry = country

    if _save:
        record.save()

    with open(path_data_file(datadir, 'bank_configuration-bank.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            record = BankConfiguration(1)

            country = get_country(row['bank_country'])

            if country:
                record.bank_country = country
                record.save()

    with open(path_data_file(datadir, 'bank_configuration-account.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            record = AccountConfiguration(1)

            currency = get_currency(row['account_currency'])

            if currency:
                record.account_currency = currency
                record.save()

    record = CompanyConfiguration(1)
    _save = False

    with open(path_data_file(datadir, 'company_configuration.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:

            currency = get_currency(row['company_currency'])
            if currency:
                _save = True
                record.company_currency = currency

            # note: package pytz must be installed on server (otherwise comment out timezone field attribution)
            timezone = row['company_timezone']
            if timezone:
                _save = True
                record.company_timezone = timezone

    if _save:
        record.save()

    with open(path_data_file(datadir, 'condo_payment_group_configuration.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            record = CondoPaymentGroupConfiguration(1)

            record.sepa_batch_booking_selection = (
                row['sepa_batch_booking_selection'] if row['sepa_batch_booking_selection'] != pgnull else None
            )
            record.sepa_charge_bearer = row['sepa_charge_bearer'] if row['sepa_charge_bearer'] != pgnull else None

            record.save()

    with open(path_data_file(datadir, 'condo_payment_sepa_mandate_configuration.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            record = MandateConfiguration(1)

            record.scheme = row['scheme'] if row['scheme'] != pgnull else None
            record.type = row['type'] if row['type'] != pgnull else None

            record.save()

    # End default values

    with open(path_data_file(datadir, 'res_group.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        table['res_group'] = list(csvreader)

    with open(path_data_file(datadir, 'res_user.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            users = User.find(
                [('active', 'in', (True, False)), ('name', '=', row['name']), ('login', '=', row['login'])]
            )
            if not users:
                record = User(
                    active=False if (row['active'] == 'f' or row['active'] == 0) else True,
                    # company
                    email=row['email'] if row['email'] != pgnull else None,
                    language=get_lang(row['language']),
                    login=row['login'] if row['login'] != pgnull else None,
                    # menu = row['menu'] if row['menu']!=pgnull else None,
                    name=row['name'] if row['name'] != pgnull else None,
                    # employee
                )

                with open(path_data_file(datadir, 'res_user-res_group.csv'), 'r') as _csvfile:
                    _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                    for _row in filter(lambda f: f['user'] == row['id'], _csvreader):
                        r = next(filter(lambda f: f['id'] == _row['group'], table['res_group']), None)

                        groups = Group.find([('name', '=', r['name'])])
                        if groups and len(groups) == 1:
                            record.groups.append(groups[0])
                        else:
                            logging.error('<res_user-res_group>: Group not found id: ' + row['id'])

                record.save()

                # copy password_hash of this user
                with Transaction().start(
                    dbname, 0, _nocache=True
                ) as transaction, transaction.connection.cursor() as cursor:
                    user = Table('res_user')
                    cursor.execute(
                        *user.update(
                            columns=[user.password_hash], values=[row['password_hash']], where=user.id == record.id
                        )
                    )
            else:
                record = users[0]

            iduser[row['id']] = record.id

    with open(path_data_file(datadir, 'condo_factor.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        table['condo_factor'] = list(csvreader)

    with open(path_data_file(datadir, 'party_category.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        table['party_category'] = list(csvreader)

    with open(path_data_file(datadir, 'party_relation_type.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')
        table['party_relation_type'] = list(csvreader)

    with open(path_data_file(datadir, 'ir_ui_view_search.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            user = User(iduser[row['user']])
            record = ViewSearch(
                domain=row['domain'].encode('utf').decode('unicode_escape'),
                model=row['model'],
                name=row['name'],
                user=user,
            )
            record.save()

    for row in sorted(table['party_category'], key=lambda f: f['id']):
        parent = None
        if row['parent'] != pgnull:
            r = next(filter(lambda f: f['id'] == row['parent'], table['party_category']), None)

            if r:
                parent = Category(r['_new_id'])
            else:
                logging.error('<party_category>: Category not found id: ' + row['id'])

        category = Category.find([('name', '=', row['name']), ('active', 'in', (True, False))])
        if (category is None) or len(category) == 0:
            record = Category(
                active=False if (row['active'] == 'f' or row['active'] == 0) else True, name=row['name'], parent=parent
            )
            record.save()

            row['_new_id'] = record.id
            set_translation(row, 'party.category', 'name')

        elif len(category) == 1:
            row['_new_id'] = category[0].id
            set_translation(row, 'party.category', 'name')
        else:
            logging.error('<party_category>: Category not found with name: ' + row['name'])

    for row in sorted(table['party_relation_type'], key=lambda f: f['id']):

        record = PartyRelationType(name=row['name'])
        record.save()
        row['_new_id'] = record.id

        set_translation(row, 'party.relation.type', 'name')

    for row in table['party_relation_type']:
        if row['reverse'] != pgnull:

            r = next(filter(lambda f: f['id'] == row['reverse'], table['party_relation_type']), None)
            reverse = PartyRelationType(r['_new_id'])

            record = PartyRelationType(row['_new_id'])
            record.reverse = reverse
            record.save()

    with open(path_data_file(datadir, 'bank.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader, desc=desc(ni).format('bank', ni, nt), total=rawgencount(path_data_file(datadir, 'bank.csv')) - 1
        ):
            banks = None

            country = get_country(row['country'])
            if country and row['code']:
                banks = Bank.find([('code', '=', row['code']), ('country', '=', country.id)])

            if banks and len(banks) == 1:
                bank = banks[0]

                bank.subset = False if (row['subset'] == 'f' or row['subset'] == 0) else True
                bank.country_subset = get_country(row['country_subset']) if row['country_subset'] != pgnull else None
                bank.save()

            else:
                logging.error('<bank>: Bank not found code: ' + row['code'])

    with open(path_data_file(datadir, 'bank_account.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('bank_account', ni, nt),
            total=rawgencount(path_data_file(datadir, 'bank_account.csv')) - 1,
        ):
            bank, currency = None, get_currency(row['currency'])

            with open(path_data_file(datadir, 'bank.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                _row = next(filter(lambda f: f['id'] == row['bank'], _csvreader), None)

                if _row:
                    country = get_country(_row['country'])
                    if country:
                        banks = Bank.find([('code', '=', _row['code']), ('country', '=', country.id)])
                else:
                    logging.error('<bank>: Bank not found id: ' + row['bank'])

                if banks and len(banks) == 1:
                    bank = banks[0]
                else:
                    logging.error('<bank>: Bank not found code: ' + _row['code'])

            record = BankAccount(
                active=False if (row['active'] == 'f' or row['active'] == 0) else True, bank=bank, currency=currency
            )

            with open(path_data_file(datadir, 'bank_account_number.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['account'] == row['id'], _csvreader):
                    record.numbers.new(
                        number=_row['number'],
                        sequence=int(_row['sequence']) if _row['sequence'] != pgnull else None,
                        type=_row['type'],
                    )

                    # check if spanish bank account number is correct
                    if bank.code[0:2] == 'ES' and _row['number'][5:9] != bank.code[2:6]:
                        logging.warning(
                            '<bank_account_number>: ' + _row['number'] + ' does not match bank ' + bank.code
                        )

                record.save()
                idaccount[row['id']] = record.id

    # Check orphan account_numbers
    with open(path_data_file(datadir, 'bank_account-party_party.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        accounts = [f['account'] for f in csvreader]

    with open(path_data_file(datadir, 'bank_account_number.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        orphans = [f['number'] for f in csvreader if f['account'] not in accounts]
        for row in orphans:
            logging.warning('<bank_account_number>: Bank Account Number ' + row + ' without owner')

    # skip list of party.address.format in ir_model_data (loaded by modules)
    model_data_noupdate = {}
    model_data_update = {}
    with open(path_data_file(datadir, 'ir_model_data.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for item in filter(lambda f: f['model'] == 'party.address.format', csvreader):
            if item['noupdate'] == 'f' or item['noupdate'] == 0:
                model_data_noupdate[item['db_id']] = item['fs_id']
            else:
                model_data_update[item['db_id']] = item['fs_id']

    with open(path_data_file(datadir, 'party_address_format.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('party_address_format', ni, nt),
            total=rawgencount(path_data_file(datadir, 'party_address_format.csv')) - 1,
        ):

            if row['id'] in model_data_noupdate:
                continue

            if row['id'] in model_data_update:
                if row['write_date'] != pgnull and row['write_uid'] != pgnull:
                    records = ModelData.find(
                        [('model', '=', 'party.address.format'), ('fs_id', '=', model_data_update[row['id']])]
                    )

                    if records and len(records) == 1:
                        record = AddressFormat(records[0].db_id)

                        country = get_country(row['country'])
                        lang = get_lang(row['language'])

                        record.active = False if (row['active'] == 'f' or row['active'] == 0) else True
                        record.country = country
                        record.format_ = (
                            row['format_'].replace('\\r\\n', '\n').replace('\\n', '\n')
                            if row['format_'] != pgnull
                            else None
                        )
                        record.language = lang
                        record.save()

                        logging.warning(
                            '<party_address_format>: Record updated with country: '
                            + country.name
                            + ' and with fs_id: '
                            + model_data_update[row['id']]
                        )
                    else:
                        logging.error(
                            '<party_address_format>: Record with fs_id ' + model_data_update[row['id']] + ' not found'
                        )

                continue

            country = get_country(row['country'])
            lang = get_lang(row['language'])

            record = AddressFormat(
                active=False if (row['active'] == 'f' or row['active'] == 0) else True,
                country=country,
                format_=row['format_'] if row['format_'] != pgnull else None,
                language=lang,
            )
            record.save()

    # skip list of parties in ir_model_data (loaded by modules)
    model_data_noupdate = {}
    model_data_update = {}
    with open(path_data_file(datadir, 'ir_model_data.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for item in filter(lambda f: f['model'] == 'party.party', csvreader):
            if item['noupdate'] == 'f' or item['noupdate'] == 0:
                model_data_noupdate[item['db_id']] = item['fs_id']
            else:
                model_data_update[item['db_id']] = item['fs_id']

    # get id of lang field property in module party
    with open(path_data_file(datadir, 'ir_model_field.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        r = next(filter(lambda f: f['relation'] == 'ir.lang' and f['module'] == 'party', csvreader), None)
        flang = r['id']

    with open(path_data_file(datadir, 'party_party.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('party_party', ni, nt),
            total=rawgencount(path_data_file(datadir, 'party_party.csv')) - 1,
        ):

            if row['id'] in model_data_noupdate:
                continue

            with open(path_data_file(datadir, 'party_party_lang.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):
                    lang = get_lang(_row['lang'])

            record = Party(
                active=False if (row['active'] == 'f' or row['active'] == 0) else True, lang=lang, name=row['name']
            )

            with open(path_data_file(datadir, 'party_address.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                numaddresses, i = 0, 0
                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):

                    numaddresses += 1
                    if (
                        sum(
                            1
                            for a in [_row[b] for b in ['name', 'street', 'zip', 'city', 'subdivision']]
                            if a not in [pgnull, u'']
                        )
                        == 0
                    ):
                        continue

                    country = get_country(_row['country'])
                    subdivision = (
                        get_subdivision(_row['subdivision'], country) if _row['subdivision'] != pgnull else None
                    )

                    if i != 0:
                        _record = Address(
                            active=True,
                            city=_row['city'] if _row['city'] != pgnull else None,
                            country=country,
                            name=_row['name'] if _row['name'] != pgnull else None,
                            sequence=int(_row['sequence']) if _row['sequence'] != pgnull else None,
                            street=_row['street'] if _row['street'] != pgnull else None,
                            subdivision=subdivision,
                            zip=_row['zip'] if _row['zip'] != pgnull else None,
                        )
                        record.addresses.append(_record)

                    else:
                        record.addresses[0].active = True
                        record.addresses[0].city = _row['city'] if _row['city'] != pgnull else None
                        record.addresses[0].country = country
                        record.addresses[0].name = _row['name'] if _row['name'] != pgnull else None
                        record.addresses[0].street = _row['street'] if _row['street'] != pgnull else None
                        record.addresses[0].subdivision = subdivision
                        record.addresses[0].zip = _row['zip'] if _row['zip'] != pgnull else None

                    i += 1

            num = numaddresses - i
            if num != 0 and i != 0:
                logging.info(
                    '<party_address>: {0} empty addresses not created for party name: '.format(num) + row['name']
                )

            with open(path_data_file(datadir, 'party_contact_mechanism.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):
                    _record = ContactMechanism(
                        active=False if (_row['active'] == 'f' or _row['active'] == 0) else True,
                        comment=_row['comment'].replace('\\r\\n', '\n').replace('\\n', '\n')
                        if _row['comment'] != pgnull
                        else None,
                        name=_row['name'] if _row['name'] != pgnull else None,
                        sequence=int(_row['sequence']) if _row['sequence'] != pgnull else None,
                        type=_row['type'] if _row['type'] != pgnull else None,
                        value=_row['value'] if _row['value'] != pgnull else None,
                    )
                    record.contact_mechanisms.append(_record)

            seen = set()

            with open(path_data_file(datadir, 'bank_account-party_party.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['owner'] == row['id'], _csvreader):
                    if _row['account'] in seen:
                        logging.error(
                            '<bank_account-party_party>: user already defined this bank account: ' + _row['account']
                        )
                        continue
                    else:
                        seen.add(_row['account'])

                    account = BankAccount(idaccount[_row['account']])

                    if account:
                        record.bank_accounts.append(account)
                    else:
                        logging.errort('<bank_account-party_party>: Bank Account not found id: ' + _row['account'])

            with open(path_data_file(datadir, 'party_identifier.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):
                    _record = Identifier(
                        code=_row['code'] if _row['code'] != pgnull else None,
                        type=_row['type'] if _row['type'] != pgnull else None,
                    )
                    record.identifiers.append(_record)

            with open(path_data_file(datadir, 'party_category_rel.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):
                    r = next(filter(lambda f: f['id'] == _row['category'], table['party_category']), None)
                    if r:
                        category = Category(r['_new_id'])
                        record.categories.append(category)
                    else:
                        logging.error('<party_category_rel>: Category not found id: ' + _row['category'])

            record.save()
            idparties[row['id']] = record.id

            _save, i = False, 0

            with open(path_data_file(datadir, 'party_address.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['party'] == row['id'], _csvreader):
                    if (
                        sum(
                            1
                            for a in [_row[b] for b in ['name', 'street', 'zip', 'city', 'subdivision']]
                            if a not in [pgnull, u'']
                        )
                        == 0
                    ):
                        continue
                    if len(record.addresses) > i:
                        idaddress[_row['id']] = record.addresses[i].id
                        if _row['active'] == 'f' or _row['active'] == 0:
                            record.addresses[i].active = False
                            _save = True
                    i += 1

            if i == 0 and len(record.addresses):
                logging.warning(
                    '<party_address>: Deleted {0} empty addresses of owner name: '.format(len(record.addresses))
                    + row['name']
                )
                for address in record.addresses:
                    address.delete()

            if _save:
                record.save()

    with open(path_data_file(datadir, 'party_relation.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            to = get_party(row['to'])
            from_ = get_party(row['from_'])

            r = next(filter(lambda f: f['id'] == row['type'], table['party_relation_type']), None)
            type_ = PartyRelationType(r['_new_id'])

            record = PartyRelation(from_=from_, to=to, type=type_)
            record.save()

    # note: package pytz must be installed on server (otherwise comment out timezone field attribution)
    with open(path_data_file(datadir, 'company_company.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('company_company', ni, nt),
            total=rawgencount(path_data_file(datadir, 'company_company.csv')) - 1,
        ):

            currency = get_currency(row['currency'])
            party = get_party(row['party'])
            accountnumber = get_bankaccountnumber(row['company_account_number'])

            if party:
                record = Company(
                    company_account_number=accountnumber,
                    company_sepa_batch_booking_selection=row['company_sepa_batch_booking_selection']
                    if row['company_sepa_batch_booking_selection'] != pgnull
                    else None,
                    company_sepa_charge_bearer=row['company_sepa_charge_bearer']
                    if row['company_sepa_charge_bearer'] != pgnull
                    else None,
                    creditor_business_code=row['creditor_business_code']
                    if row['creditor_business_code'] != pgnull
                    else None,
                    currency=currency,
                    footer=row['footer'] if row['footer'] != pgnull else '',
                    header=row['header'] if row['header'] != pgnull else '',
                    is_condo=False if (row['is_condo'] == 'f' or row['is_condo'] == 0) else True,
                    parent=None,
                    party=party,
                    sepa_creditor_identifier=row['sepa_creditor_identifier']
                    if row['sepa_creditor_identifier'] != pgnull
                    else None,
                    timezone=row['timezone'] if row['timezone'] != pgnull else None,
                )

                for _row in filter(lambda f: f['company'] == row['id'], table['condo_factor']):
                    _record = CondoFactor(
                        name=_row['name'] if _row['name'] != pgnull else None,
                        notes=_row['notes'] if _row['notes'] != pgnull else None,
                    )
                    record.condofactors.append(_record)

                with open(path_data_file(datadir, 'condo_unit.csv'), 'r') as _csvfile:
                    _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                    for _row in sorted(
                        filter(lambda f: f['company'] == row['id'], _csvreader), key=lambda f: f['name']
                    ):
                        _record = CondoUnit(name=_row['name'] if _row['name'] != pgnull else None)
                        record.units.append(_record)

                record.save()
                idcompany[row['id']] = record.id

            else:
                logging.error('<company_company>: Company not found with id: ' + row['id'])

    # set parent of companies
    with open(path_data_file(datadir, 'company_company.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:
            record = get_company(row['id'])

            if row['parent'] != pgnull:
                if idcompany[row['parent']]:
                    parent = get_company(row['parent'])
                    record.parent = parent
                    record.save()
                else:
                    logging.error('<company_company>: Company not found id: ' + row['id'])

    with open(path_data_file(datadir, 'condo_payment_sepa_mandate.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('condo_payment_sepa_mandate', ni, nt),
            total=rawgencount(path_data_file(datadir, 'condo_payment_sepa_mandate.csv')) - 1,
        ):
            company = get_company(row['company'])
            party = get_party(row['party'])
            accountnumber = get_bankaccountnumber(row['account_number'])

            record = Mandate(
                account_number=accountnumber,
                company=company,
                identification=row['identification'] if row['identification'] != pgnull else None,
                party=party,
                scheme=row['scheme'] if row['scheme'] != pgnull else None,
                signature_date=datetime.strptime(row['signature_date'], "%Y-%m-%d")
                if row['signature_date'] != pgnull
                else None,
                state=row['state'] if row['state'] != pgnull else None,
                type=row['type'] if row['type'] != pgnull else None,
            )
            if accountnumber.account.owners and len(accountnumber.account.owners):
                record.save()
            else:
                logging.error(
                    '<condo_payment_sepa_mandate>: Bank account number '
                    + accountnumber.number
                    + ' without owner but used in mandate '
                    + (row['identification'] if row['identification'] != pgnull else None)
                    + ' with state '
                    + (row['state'] if row['state'] != pgnull else None)
                )
            idmandate[row['id']] = record.id

    with open(path_data_file(datadir, 'condo_unit.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('condo_unit', ni, nt),
            total=rawgencount(path_data_file(datadir, 'condo_unit.csv')) - 1,
        ):

            unit, = CondoUnit.find([('name', '=', row['name']), ('company', '=', idcompany[row['company']])])

            if unit:
                with open(path_data_file(datadir, 'condo_unit-factor.csv'), 'r') as _csvfile:
                    _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                    for _row in filter(lambda f: f['unit'] == row['id'], _csvreader):
                        for __row in filter(lambda f: f['id'] == _row['condofactor'], table['condo_factor']):
                            company = get_company(__row['company'])
                            condofactor, = CondoFactor.find(
                                [('name', '=', __row['name']), ('company', '=', company.id)]
                            )
                        if condofactor:
                            _record = UnitFactor(
                                condofactor=condofactor,
                                value=Decimal(_row['value']) if _row['value'] != pgnull else None,
                            )
                            unit.factors.append(_record)
                        else:
                            logging.error('<condo_unit-factor>: Unit Factor not found id: ' + row['id'])

                with open(path_data_file(datadir, 'condo_party.csv'), 'r') as _csvfile:
                    _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                    for _row in filter(lambda f: f['unit'] == row['id'], _csvreader):
                        address = Address(idaddress[_row['address']]) if _row['address'] in idaddress else None
                        party = get_party(_row['party'])
                        mandate = Mandate(idmandate[_row['mandate']]) if _row['mandate'] != pgnull else None

                        if not address:
                            logging.warning(
                                '<condo_party>: Mail address not found for party id: '
                                + str(idparties[_row['party']])
                                + ' and name:'
                                + party.name
                            )

                        _record = CondoParty(
                            address=address if _row['address'] != pgnull else None,
                            party=party,
                            role=_row['role'] if _row['role'] != pgnull else None,
                            mandate=mandate,
                        )
                        unit.condoparties.append(_record)

                unit.save()
                idunits[row['id']] = unit.id
            else:
                logging.error('<condo_unit>: Unit not found id: ' + row['id'])

    with open(path_data_file(datadir, 'condo_payment_pain.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('condo_payment_pain', ni, nt),
            total=rawgencount(path_data_file(datadir, 'condo_payment_pain.csv')) - 1,
        ):
            company = get_company(row['company'])

            record = CondoPain(
                company=company,
                country_subset=get_country(row['country_subset']) if row['country_subset'] != pgnull else None,
                message=row['message'].replace('\\r\\n', '\n').replace('\\n', '\n')
                if row['message'] != pgnull
                else None,
                reference=row['reference'] if row['reference'] != pgnull else None,
                sepa_receivable_flavor=row['sepa_receivable_flavor']
                if row['sepa_receivable_flavor'] != pgnull
                else None,
                state=row['state'] if row['state'] != pgnull else None,
                subset=False if (row['subset'] == 'f' or row['subset'] == 0) else True,
            )

            record.save()
            idpains[row['id']] = record.id

    with open(path_data_file(datadir, 'condo_payment_group.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('condo_payment_group', ni, nt),
            total=rawgencount(path_data_file(datadir, 'condo_payment_group.csv')) - 1,
        ):

            company = get_company(row['company'])
            pain = CondoPain(idpains[row['pain']]) if row['pain'] != pgnull else None

            # case condo_payment_group is included in any pain message
            if not pain:
                logging.error(
                    '<condo_payment_group>: Payment Group with reference '
                    + row['reference']
                    + ' of condominium '
                    + company.party.name
                    + ' not found'
                )

            accountnumber = get_bankaccountnumber(row['account_number'])

            record = CondoPaymentGroup(
                account_number=accountnumber,
                company=company,
                date=datetime.strptime(row['date'], "%Y-%m-%d") if row['date'] != pgnull else None,
                message=row['message'].replace('\\r\\n', '\n').replace('\\n', '\n')
                if row['message'] != pgnull
                else None,
                pain=pain,
                reference=row['reference'] if row['reference'] != pgnull else None,
                sepa_batch_booking=False
                if (row['sepa_batch_booking'] == 'f' or row['sepa_batch_booking'] == 0)
                else True,
                sepa_charge_bearer=row['sepa_charge_bearer'] if row['sepa_charge_bearer'] != pgnull else None,
            )

            with open(path_data_file(datadir, 'condo_payment.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['group'] == row['id'], _csvreader):

                    currency = get_currency(_row['currency'])
                    party = get_party(_row['party'])
                    unit = CondoUnit(idunits[_row['unit']]) if _row['unit'] != pgnull else None
                    mandate = Mandate(idmandate[_row['mandate']]) if _row['mandate'] != pgnull else None

                    if len(_row['description']) > 140:
                        description = re.sub(r'\\*\\n', '', _row['description'])
                        if len(description) > 140:
                            description = description[:140]
                            logging.error('<condo_payment>: description field trimmed to 140 characters')
                    else:
                        description = _row['description'].replace('\\r\\n', '').replace('\\n', '')

                    if len(description) != len(_row['description']):
                        logging.warning(
                            '<condo_payment>: description field size changed'
                            + '\ncondominium:  "'
                            + unit.company.party.name
                            + '"\nunit :        "'
                            + unit.name
                            + '"\npaymentgroup: "'
                            + record.reference
                            + '"\noriginal: "'
                            + _row['description']
                            + '"\nnew:      "'
                            + description
                            + '"'
                        )

                    _record = CondoPayment(
                        amount=Decimal(_row['amount']) if _row['amount'] != pgnull else None,
                        currency=currency,
                        date=datetime.strptime(_row['date'], "%Y-%m-%d") if _row['date'] != pgnull else None,
                        description=description if _row['description'] != pgnull else None,
                        party=party,
                        sepa_end_to_end_id=_row['sepa_end_to_end_id'] if _row['sepa_end_to_end_id'] != pgnull else None,
                        mandate=mandate,
                        state=_row['state'] if _row['state'] != pgnull else None,
                        type=_row['type'] if _row['type'] != pgnull else None,
                        unit=unit,
                    )
                    record.payments.append(_record)

            record.save()

    # skip list of holidays.calendar in ir_model_data (loaded by modules)
    model_data_noupdate = {}
    model_data_update = {}
    with open(path_data_file(datadir, 'ir_model_data.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for item in filter(lambda f: f['model'] == 'holidays.calendar', csvreader):
            if item['noupdate'] == 'f' or item['noupdate'] == 0:
                model_data_noupdate[item['db_id']] = item['fs_id']
            else:
                model_data_update[item['db_id']] = item['fs_id']

    with open(path_data_file(datadir, 'holidays_calendar.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('holidays_calendar', ni, nt),
            total=rawgencount(path_data_file(datadir, 'holidays_calendar.csv')) - 1,
        ):

            if row['id'] in model_data_noupdate:
                records = ModelData.find(
                    [('model', '=', 'holidays.calendar'), ('fs_id', '=', model_data_noupdate[row['id']])]
                )
                if records and len(records) == 1:
                    idhlds[row['id']] = records[0].db_id
                else:
                    logging.warning(
                        '<holidays_calendar>: Calendar not found with fs_id: ' + model_data_noupdate[row['id']]
                    )

                continue

            if row['id'] in model_data_update:
                records = ModelData.find(
                    [('model', '=', 'holidays.calendar'), ('fs_id', '=', model_data_update[row['id']])]
                )
                if records and len(records) == 1:
                    calendar = Holidays(records[0].db_id)

                    idhlds[row['id']] = calendar.id

                    if row['write_date'] != pgnull and row['write_uid'] != pgnull:
                        owner = User(iduser[row['owner']])

                        calendar.descripttion = row['description'] if row['description'] != pgnull else None
                        calendar.name = row['name']
                        calendar.owner = owner
                        calendar.save()

                        logging.warning(
                            '<holidays_calendar>: Record updated with name: '
                            + calendar.name
                            + ' and fs_id: '
                            + model_data_update[row['id']]
                        )
                else:
                    logging.error(
                        '<holidays_calendar>: Record with fs_id ' + model_data_update[row['id']] + ' not found'
                    )

                continue

            owner = User(iduser[row['owner']])
            record = Holidays(
                description=row['description'] if row['description'] != pgnull else None, name=row['name'], owner=owner
            )

            with open(path_data_file(datadir, 'holidays_event.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['calendar'] == row['id'], _csvreader):

                    _record = HolidaysEvent(
                        description=_row['description'] if _row['description'] != pgnull else None,
                        dtend=datetime.strptime(_row['dtend'], '%Y-%m-%d') if _row['dtend'] != pgnull else None,
                        dtstart=datetime.strptime(_row['dtstart'], '%Y-%m-%d'),
                        status=_row['status'] if _row['status'] != pgnull else None,
                        summary=_row['summary'] if _row['summary'] != pgnull else None,
                        # uuid
                    )

                    with open(path_data_file(datadir, 'holidays_event_rrule.csv'), 'r') as __csvfile:
                        __csvreader = csv.DictReader(__csvfile, delimiter='\t')

                        for __row in filter(lambda f: f['event'] == _row['id'], __csvreader):
                            __record = HolidaysEventRRule(
                                count=__row['count'] if __row['count'] != pgnull else None,
                                byday=__row['byday'] if __row['byday'] != pgnull else None,
                                byeaster=__row['byeaster'] if __row['byeaster'] != pgnull else None,
                                bymonth=__row['bymonth'] if __row['bymonth'] != pgnull else None,
                                bymonthday=__row['bymonthday'] if __row['bymonthday'] != pgnull else None,
                                bysetpos=__row['bysetpos'] if __row['bysetpos'] != pgnull else None,
                                byyearday=__row['byyearday'] if __row['byyearday'] != pgnull else None,
                                byweekno=__row['byweekno'] if __row['byweekno'] != pgnull else None,
                                freq=__row['freq'] if __row['freq'] != pgnull else None,
                                interval=__row['interval'] if __row['interval'] != pgnull else None,
                                until=datetime.strptime(__row['until'], '%Y-%m-%d')
                                if __row['until'] != pgnull
                                else None,
                                wkst=__row['wkst'] if __row['wkst'] != pgnull else None,
                            )
                            _record.rrules.append(__record)

                    record.events.append(_record)

            record.save()
            idhlds[row['id']] = record.id

    with open(path_data_file(datadir, 'holidays_calendar.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:

            _save = False
            record = Holidays(idhlds[row['id']])

            if row['id'] not in model_data_noupdate and row['parent'] != pgnull:
                parent = Holidays(idhlds[row['parent']])

                record.parent = parent
                _save = True

            with open(path_data_file(datadir, 'holidays_calendar-read-res_user.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['calendar'] == row['id'], _csvreader):
                    user = User(iduser[_row['user']])

                    if user.id not in record.read_users:
                        record.read_users.append(user)
                        _save = True

            with open(path_data_file(datadir, 'holidays_calendar-write-res_user.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['calendar'] == row['id'], _csvreader):
                    user = User(iduser[_row['user']])

                    if user.id not in record.read_users:
                        record.write_users.append(user)
                        _save = True

            if _save:
                record.save()

    with open(path_data_file(datadir, 'recurrence.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        ni += 1
        for row in tqdm(
            csvreader,
            desc=desc(ni).format('recurrence', ni, nt),
            total=rawgencount(path_data_file(datadir, 'recurrence.csv')) - 1,
        ):

            record = Recurrence(
                active=False if (row['active'] == 'f' or row['active'] == 0) else True,
                days=int(row['days']) if row['days'] != pgnull else None,
                description=row['description'] if row['description'] != pgnull else None,
                direction=row['direction'] if row['direction'] != pgnull else None,
                dtstart=datetime.strptime(row['dtstart'], '%Y-%m-%d %H:%M:%S'),
                leapdays=int(row['leapdays']) if row['leapdays'] != pgnull else None,
                months=int(row['months']) if row['months'] != pgnull else None,
                name=row['name'] if row['name'] else None,
                weekday=row['weekday'] if row['weekday'] != pgnull else None,
                weeks=int(row['weeks']) if row['weeks'] != pgnull else None,
                years=int(row['years']) if row['years'] != pgnull else None,
            )

            with open(path_data_file(datadir, 'recurrence_event.csv'), 'r') as _csvfile:
                _csvreader = csv.DictReader(_csvfile, delimiter='\t')

                for _row in filter(lambda f: f['recurrence'] == row['id'], _csvreader):

                    user = User(iduser[_row['user']])
                    request_user = User(iduser[_row['request_user']])

                    _record = RecurrenceEvent(
                        args=_row['args'] if _row['args'] != pgnull else None,
                        function=_row['function'] if _row['function'] != pgnull else None,
                        model=_row['model'] if _row['model'] != pgnull else None,
                        name=_row['name'],
                        number_calls=int(_row['number_calls']) if _row['number_calls'] != pgnull else None,
                        repeat_missed=False if (_row['repeat_missed'] == 'f' or _row['repeat_missed'] == 0) else True,
                        request_user=request_user,
                        user=user,
                    )

                    with open(path_data_file(datadir, 'recurrence_date.csv'), 'r') as __csvfile:
                        __csvreader = csv.DictReader(__csvfile, delimiter='\t')

                        for __row in filter(lambda f: f['event'] == _row['id'], __csvreader):
                            holidays = Holidays(idhlds[__row['holidays']]) if __row['holidays'] != pgnull else None

                            __record = RecurrenceDate(
                                delta_days=int(__row['delta_days']),
                                holidays=holidays,
                                name=__row['name'],
                                trigger=False if (__row['trigger'] == 'f' or __row['trigger'] == 0) else True,
                            )

                            _record.dates.append(__record)

                    with open(path_data_file(datadir, 'recurrence_event-company_company.csv'), 'r') as __csvfile:
                        __csvreader = csv.DictReader(__csvfile, delimiter='\t')

                        for __row in filter(lambda f: f['event'] == _row['id'], __csvreader):
                            company = get_company(__row['company'])

                            _record.companies.append(company)

                    record.events.append(_record)

            record.save()

    with open(path_data_file(datadir, 'res_user.csv'), 'r') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter='\t')

        for row in csvreader:

            if row['main_company'] != pgnull or row['company'] != pgnull:
                user = User(iduser[row['id']])

                if row['main_company'] != pgnull:
                    main_company = Company(idcompany[row['main_company']])
                    user.main_company = main_company

                if row['company'] != pgnull:
                    company = Company(idcompany[row['company']])
                    user.company = company

                user.save()


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        populate(str(sys.argv[1]))
    else:
        sys.exit("Usage: python " + str(sys.argv[0]) + " uri")
