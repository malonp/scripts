#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

from proteus import config, Model

import os

import sys
import csv
#http://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
csv.field_size_limit(sys.maxsize)

import unicodecsv
from decimal import Decimal
from datetime import datetime

if len(sys.argv)==2:
    try:
        config = config.set_trytond(str(sys.argv[1]))
    except:
        sys.exit("Error: invalid uri " + sys.argv[1])
else:
    sys.exit("Error: invalid arguments number trytond_begin.py uri")

#config = config.set_trytond('sqlite://')
#config = config.set_trytond(config_file='/etc/tryton/trytond.conf', database='admon', user='admin')
#config = config.set_xmlrpc('https://user:passwd@ip:port/databasename')

Address = Model.get('party.address')
Bank = Model.get('bank')
BankAccount = Model.get('bank.account')
BankAccountNumber = Model.get('bank.account.number')
Category = Model.get('party.category')
Company = Model.get('company.company')
CondoFactor = Model.get('condo.factor')
CondoPain = Model.get('condo.payment.pain')
CondoParty = Model.get('condo.party')
CondoPayment = Model.get('condo.payment')
CondoPaymentGroup = Model.get('condo.payment.group')
CondoUnit = Model.get('condo.unit')
ContactMechanism = Model.get('party.contact_mechanism')
Country = Model.get('country.country')
Subdivision = Model.get('country.subdivision')
Currency = Model.get('currency.currency')
Identifier = Model.get('party.identifier')
Lang = Model.get('ir.lang')
Mandate = Model.get('condo.payment.sepa.mandate')
Party = Model.get('party.party')
PartyRelation = Model.get('party.relation.all')
PartyRelationType = Model.get('party.relation.type')
Sequence = Model.get('ir.sequence')
Subdivision = Model.get('country.subdivision')
Translation = Model.get('ir.translation')
UnitFactor = Model.get('condo.unit-factor')
User = Model.get('res.user')
ViewSearch = Model.get('ir.ui.view_search')

(es,) = Lang.find([('code', '=', 'es_ES')])

def path_data_file(name):
    return os.path.join(os.path.dirname(__file__), 'data', name)

r = unicodecsv.reader(file(path_data_file('party_party.csv')), delimiter='\t', encoding='utf-8')
rparty = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_address.csv')), delimiter='\t', encoding='utf-8')
raddress = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_contact_mechanism.csv')), delimiter='\t', encoding='utf-8')
rcontact = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('bank.csv')), delimiter='\t', encoding='utf-8')
rbank = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('bank_account.csv')), delimiter='\t', encoding='utf-8')
raccount = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('bank_account_number.csv')), delimiter='\t', encoding='utf-8')
raccountnumber = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('bank_account-party_party.csv')), delimiter='\t', encoding='utf-8')
raccountnumberparty = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_identifier.csv')), delimiter='\t', encoding='utf-8')
ridentifier = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_category.csv')), delimiter='\t', encoding='utf-8')
rcategory = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_category_rel.csv')), delimiter='\t', encoding='utf-8')
rcategoryrel = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_relation.csv')), delimiter='\t', encoding='utf-8')
rrelation = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('party_relation_type.csv')), delimiter='\t', encoding='utf-8')
rreltype = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('company_company.csv')), delimiter='\t', encoding='utf-8')
rcompany = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_factor.csv')), delimiter='\t', encoding='utf-8')
rfactor = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_unit.csv')), delimiter='\t', encoding='utf-8')
runit = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_unit-factor.csv')), delimiter='\t', encoding='utf-8')
runitfactor = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_party.csv')), delimiter='\t', encoding='utf-8')
rcondoparty = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_payment_sepa_mandate.csv')), delimiter='\t', encoding='utf-8')
rmandate = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_payment.csv')), delimiter='\t', encoding='utf-8')
rpayment = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_payment_group.csv')), delimiter='\t', encoding='utf-8')
rpaymentgroup = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('condo_payment_pain.csv')), delimiter='\t', encoding='utf-8')
rpain = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('country_country.csv')), delimiter='\t', encoding='utf-8')
rcountry = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('country_subdivision.csv')), delimiter='\t', encoding='utf-8')
rsubdivision = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('currency_currency.csv')), delimiter='\t', encoding='utf-8')
rcurrency = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('ir_ui_view_search.csv')), delimiter='\t', encoding='utf-8')
riruiview = map(tuple, r)

r = unicodecsv.reader(file(path_data_file('ir_translation.csv')), delimiter='\t', encoding='utf-8')
rtranslation = map(tuple, r)

idaddress = {}
idcategor = {}
idcompany = {}
idcuentas = {}
idmandate = {}
idparties = {}
idpreltyp = {}
idpains   = {}

party_seq, = Sequence.find([('name', '=', 'Party')])
party_seq.number_next = 10001
party_seq.save()

pgnull = '\N'

def get_bankaccountnumber(old_id):
    #COPY bank_account_number (id, create_uid, account, create_date, sequence, number, write_uid, write_date, number_compact, type) FROM stdin;
    accountnumbers_iter = filter(lambda x:x[0]==old_id, raccountnumber)
    new_accountnumber = accountnumber = None

    if accountnumbers_iter and len(accountnumbers_iter):
        accountnumber = accountnumbers_iter[0]

    if accountnumber:
        new_accountnumbers = BankAccountNumber.find([('number', '=', accountnumber[5])])
        if new_accountnumbers and len(new_accountnumbers)==1:
            new_accountnumber = new_accountnumbers[0]
        else:
            print('Error: bank account number not found number ' + accountnumber[5])
    elif old_id!=pgnull:
        print('Error: bank account number not found id ' + old_id)
    else:
        print('Warn: bank account number not found')

    return new_accountnumber

def get_company(old_id):
    #COPY company_company (id, create_uid, create_date, parent, footer, header, write_uid, currency, write_date, timezone, party, "is_Condominium", sepa_creditor_identifier, creditor_business_code, company_sepa_batch_booking_selection, company_account_number, company_sepa_charge_bearer) FROM stdin;
    if old_id not in idcompany:
        print('Warn: company not found id: ' +  old_id)
        return None

    companies = Company.find([('id', '=', idcompany[old_id])])

    new_company = None
    if companies and len(companies)==1:
        new_company = companies[0]
    else:
        print('Error: company not found id ' + old_id)

    return new_company

def get_country(old_id):
    #COPY country_country(id, create_uid, code, create_date, name, write_uid, write_date) FROM stdin;
    country_iter = filter(lambda x:x[0]==old_id, rcountry)
    new_country = country = None

    if country_iter and len(country_iter):
        country = country_iter[0]

    if country:
        new_countries = Country.find([('code', '=', country[2])])
        if new_countries and len(new_countries)==1:
            new_country = new_countries[0]
        else:
            print('Error: pais no encontrado code ' + country[2])
    else:
        print('Error: pais no encontrado id ' + old_id)

    return new_country

def get_currency(old_id):
    #COPY currency_currency (id, code, create_date, write_date, p_sep_by_space, write_uid, active, mon_grouping, create_uid, rounding, numeric_code, n_cs_precedes, n_sign_posn, p_cs_precedes, mon_decimal_point, symbol, mon_thousands_sep, negative_sign, n_sep_by_space, positive_sign, digits, name, p_sign_posn) FROM stdin;
    currency_iter = filter(lambda x:x[0]==old_id, rcurrency)

    new_currency = currency = None
    if currency_iter and len(currency_iter):
        currency = currency_iter[0]

    if currency:
        new_currencies = Currency.find([('code', '=', currency[1])])
        if new_currencies and len(new_currencies)==1:
            new_currency = new_currencies[0]
        else:
            print('Error: currency not found code ' + currency[1])
    else:
        print('Error: currency not found id ' + old_id)

    return new_currency

def get_party(old_id):
    #COPY party_party (id, code, create_date, write_uid, create_uid, write_date, active, name) FROM stdin;
    if old_id not in idparties:
        print('Warn: party not found id: ' + old_id)
        return None

    active_parties = Party.find([('id', '=', idparties[old_id])])

    new_party = None
    if (active_parties is None) or len(active_parties)==0:
        inactive_parties = Party.find([('id', '=', idparties[old_id]), ('active', '=', False)])
        if inactive_parties and len(inactive_parties)==1:
            new_party = inactive_parties[0]
        else:
            print('Error: party not found id ' + old_id)
    else:
        new_party = active_parties[0]

    return new_party

for row in sorted(rcategory, key=lambda field: field[0]):
    padre = None
    if row[4]!=pgnull:
        if idcategor[row[4]]:
            padre, = Category.find([('id', '=', idcategor[row[4]])])
        else:
            print("Error: Verificar padre de la categoria " + row[0])

    category = Category.find([('name', '=', row[3])])
    if (category is None) or len(category)==0:
        category = Category.find([('name', '=', row[3]), ('active', '=', False)])
        if (category is None) or len(category)==0:
            #COPY party_category (id, create_uid, create_date, name, parent, write_uid, write_date, active) FROM stdin;
            categoria = Category(name = row[3],
                                 parent = padre,
                                 active = False if (row[7]=='f' or row[7]==0) else True)
            categoria.save()
            idcategor[row[0]] = categoria.id
        elif category and len(category)==1:
            idcategor[row[0]] = category[0].id

    elif category and len(category)==1:
        idcategor[row[0]] = category[0].id

#TODO category translations

for row in sorted(rreltype, key=lambda field: field[0]):
    reverse = None
    reltype = PartyRelationType(name = row[6])
    reltype.save()
    idpreltyp[row[0]] = reltype.id

    translations = filter(lambda x:x[2]==row[6], rtranslation)
    #COPY ir_translation (id, lang, src, src_md5, name, res_id, value, type, module, fuzzy, create_uid, create_date, overriding_module, write_uid, write_date) FROM stdin;
    for translation in translations:
        traduccion = Translation.find([
                                        ('src', '=', translation[2] if translation[2]!=pgnull else None),
                                        ('name', '=', translation[4] if translation[4]!=pgnull else None),
                                        ('res_id', '=', translation[5] if translation[5]!=pgnull else None),
                                        ('type', '=', translation[7] if translation[7]!=pgnull else None),
                                        ('module', '=', translation[8] if translation[8]!=pgnull else None)
                                      ])

        if traduccion and len(traduccion):
            traduccion[0].lang = translation[1]
            traduccion[0].value = translation[6]
            traduccion[0].save()
        else:
            #TODO?
            traduccion = Translation(lang = translation[1],
                                     src = translation[2],
                                     name = translation[4] if translation[4]!=pgnull else None,
                                     res_id = translation[5] if translation[5]!=pgnull else None,
                                     value = translation[6],
                                     type = translation[7] if translation[7]!=pgnull else None,
                                     module = translation[8] if translation[8]!=pgnull else None)
            traduccion.save()


for row in rreltype:
    if row[3]!=pgnull:
        reverse, = PartyRelationType.find([('id', '=', idpreltyp[row[3]])])
        reltype, = PartyRelationType.find([('id', '=', idpreltyp[row[0]])])
        reltype.reverse = reverse
        reltype.save()

for row in raccount:

    moneda = get_currency(row[4])

    bank_iter = filter(lambda x:x[0]==row[7], rbank)

    bank = None
    if bank_iter and len(bank_iter):
        bank = bank_iter[0]
        pais = get_country(bank[8])
        if pais:
            bancos = Bank.find([('code', '=', bank[7]), ('country', '=', pais.id)])
    else:
        print('Error: banco no encontrado id ' + row[7])

    if bancos and len(bancos)==1:
        banco = bancos[0]
    else:
        banco = None
        print('Error: banco no encontrado')

    #COPY bank_account (id, create_uid, create_date, write_uid, currency, write_date, active, bank) FROM stdin;
    account = BankAccount(currency = moneda,
                          active = False if (row[6]=='f' or row[6]==0) else True,
                          bank = banco)
    accountnumbers = filter(lambda x:x[2]==row[0], raccountnumber)
    #COPY bank_account_number (id, create_uid, account, create_date, sequence, number, write_uid, write_date, number_compact, type) FROM stdin;
    for accountnumber in accountnumbers:
        account.numbers.new(type = accountnumber[9],
                            sequence = int(accountnumber[4]) if accountnumber[4]!=pgnull else None,
                            number = accountnumber[5])

        if (banco.code[0:2]=='ES' and accountnumber[5][5:9] != banco.code[2:6]):
             print('Alerta: cuenta ' + accountnumber[5] + ' no coincide con banco ' + banco.code)

    account.save()
    idcuentas[row[0]] = account.id

#Check orphan account_numbers
raccountnumberparty_account = [item[2] for item in raccountnumberparty]
orphan = [item for item in raccountnumber if item[2] not in raccountnumberparty_account]
for item in orphan:
    print('Error: cuenta numero ' + item[5] + ' sin propietario')

for row in sorted(rparty, key=lambda field:(False if field[0] in [r[10] for r in rcompany] else True, field[7])):

    #bypass warning on duplicate party's name when is a bank
    categories = [c[1] for c in filter(lambda x:x[6]==row[0], rcategoryrel)]
    if u'bank' in [category[3] for category in rcategory if category[0] in categories ]:
        continue

#TODO lang
#        if row[]!=pgnull:
#            lenguaje, = Lang.find([('id', '=', 'es_ES')])

    #COPY party_party (id, code, create_date, write_uid, create_uid, write_date, active, name) FROM stdin;
    party = Party(name = row[7],
                  active = False if (row[6]=='f' or row[6]==0) else True,
                  lang = es)

    addresses_iter = filter(lambda x:x[14]==row[0], raddress)
    numaddresses = len(addresses_iter)
    addresses = filter(lambda x:sum(1 for a in [x[i] for i in [1, 3, 4, 8, 10, 11]] if a not in [pgnull, u''])!=0, addresses_iter)
    #COPY party_address (id, city, create_date, name, zip, create_uid, country, sequence, subdivision, write_uid, streetbis, street, write_date, active, party) FROM stdin;

    i = 0
    for address in addresses:
        provincia = None

        pais = get_country(address[6])

        if address[8]!=pgnull:
            # COPY country_subdivision (id, create_uid, code, create_date, name, parent, country, write_uid, write_date, type) FROM stdin;
            subdivision_iter = filter(lambda x:x[0]==address[8], rsubdivision)
            subdivision = None
            if subdivision_iter and len(subdivision_iter)==1:
                subdivision = subdivision_iter[0]

            if subdivision and subdivision[6]!=pgnull:
                pais = get_country(subdivision[6])
                if pais:
                    provincias = Subdivision.find([('code', '=', subdivision[2]),
                                                   ('country', '=', pais.id),])
                else:
                    print('Error country not found id ' + subdivision[6])

                if provincias and len(provincias)==1:
                    provincia = provincias[0]
                else:
                    provincia = None
                    print('Error: subdivision not found')

            elif subdivision is not None:
                provincias = Subdivision.find([('code', '=', subdivision[2])])
                if provincias and len(provincias)==1:
                    provincia = provincias[0]
                else:
                    provincia = None
                    print('Error: subdivision not found')
            else:
                print('Error: subdivision not found with id' + address[8])

        if i!=0:
            direccion = Address(name = address[3] if address[3]!=pgnull else None,
                                street = address[11] if address[11]!=pgnull else None,
                                streetbis = address[10] if address[10]!=pgnull else None,
                                zip = address[4] if address[4]!=pgnull else None,
                                city = address[1] if address[1]!=pgnull else None,
                                subdivision = provincia,
                                country = pais,
                                sequence = int(address[7]) if address[7]!=pgnull else None,
                                active = True)
            party.addresses.append(direccion)

        else:
            party.addresses[0].name = address[3] if address[3]!=pgnull else None
            party.addresses[0].street = address[11] if address[11]!=pgnull else None
            party.addresses[0].zip = address[4] if address[4]!=pgnull else None
            party.addresses[0].city = address[1] if address[1]!=pgnull else None
            party.addresses[0].subdivision = provincia
            party.addresses[0].country = pais
            party.addresses[0].active = True

        i += 1

    num = numaddresses - i
    if num!=0 and i!=0:
        print("Aviso: {0} direccion(es) sin datos no fueron recreadas en propietario: ".format(num) + row[7])

    contacts = filter(lambda x:x[9]==row[0], rcontact)
    for contact in contacts:
        #COPY party_contact_mechanism (id, comment, create_uid, create_date, sequence, value, write_uid, write_date, active, party, type) FROM stdin;
        contacto = ContactMechanism(comment = contact[1].replace('\\r\\n', '\n').replace('\\n', '\n') if contact[1]!=pgnull else None,
                                    value = contact[5] if contact[5]!=pgnull else None,
                                    type = contact[10] if contact[10]!=pgnull else None,
                                    sequence = int(contact[4]) if contact[4]!=pgnull else None,
                                    active = False if (contact[8]=='f' or contact[8]==0) else True)
        party.contact_mechanisms.append(contacto)

    #COPY "bank_account-party_party" (id, create_uid, account, create_date, write_uid, write_date, owner)  FROM stdin;
    accounts = filter(lambda x:x[6]==row[0], raccountnumberparty)
    seen = set()
    for account in accounts:
        if account[2] in seen:
            print("Error: Cuenta y propietarios repetidos " + account[2])
            continue
        else:
            seen.add(account[2])

        cuenta = None
        if account[2]!=pgnull and idcuentas[account[2]]:
            cuenta = BankAccount.find([('id', '=', idcuentas[account[2]])])
            if cuenta and len(cuenta)==1:
                party.bank_accounts.append(cuenta[0])
            elif (cuenta is None) or len(cuenta)==0:
                cuenta = BankAccount.find([('id', '=', idcuentas[account[2]]), ('active', '=', False)])
                if cuenta and len(cuenta)==1:
                    party.bank_accounts.append(cuenta[0])
                else:
                    print("Error: Cuenta sin propietario " + account[2])
        else:
            print("Error: Verificar cuenta bancaria " + account[2])

    identifiers = filter(lambda x:x[6]==row[0], ridentifier)
    for identifier in identifiers:
        #COPY party_identifier (id, create_uid, code, create_date, write_uid, write_date, party, type) FROM stdin;
        identificacion = Identifier(code = identifier[2] if identifier[2]!=pgnull else None,
                                    type = identifier[7] if identifier[7]!=pgnull else None)
        party.identifiers.append(identificacion)

    categories = filter(lambda x:x[6]==row[0], rcategoryrel)
    for category in categories:
        if category[1] in idcategor:
            categoria, = Category.find([('id', '=', idcategor[category[1]])])
            party.categories.append(categoria)
        else:
            print('Error: category not found id ' + category[1])

    party.save()
    idparties[row[0]] = party.id

    _save = False

    i = 0
    for address in addresses:
        if len(party.addresses) > i:
            idaddress[address[0]] = party.addresses[i].id
            if (address[13]=='f' or address[13]==0):
                party.addresses[i].active = False
                _save = True
        i += 1

    if i==0 and len(party.addresses):
        print("Aviso: borradas {0} direcciones de propietario sin direccion (activa): ".format(len(party.addresses)) + row[7])
        for address in party.addresses:
            address.delete()

    if _save:
        party.save()


for row in rrelation:
    tercero_to = get_party(row[4])
    tercero_from = get_party(row[6])

    tipo, = PartyRelationType.find([('id', '=', idpreltyp[row[7]])])

    relation = PartyRelation(to = tercero_to,
                             from_ = tercero_from,
                             type = tipo)
    relation.save()

#note: package pytz must be installed on server (otherwise comment out timezone field attribution)
for row in sorted(rcompany, key=lambda field: filter(lambda x:x[0]==field[10], rparty)[0][7]):
    moneda = get_currency(row[7])
    tercero = get_party(row[10])
    accountnumber = get_bankaccountnumber(row[15])

    if tercero:
        compania = Company(parent = None,
                           footer = row[4] if row[4]!=pgnull else '',
                           header = row[5] if row[5]!=pgnull else '',
                           currency = moneda,
                           timezone = row[9] if row[9]!=pgnull else None,
                           party = tercero,
                           is_Condominium = False if (row[11]=='f' or row[11]==0) else True,
                           creditor_business_code = row[13] if row[13]!=pgnull else None,
                           sepa_creditor_identifier = row[12] if row[12]!=pgnull else None,
                           company_sepa_batch_booking_selection = row[14] if row[14]!=pgnull else None,
                           company_account_number = accountnumber,
                           company_sepa_charge_bearer = row[16] if row[16]!=pgnull else None,
                           )

        factors = filter(lambda x:x[7]==row[0], rfactor)
        for factor in factors:
            #COPY condo_factor (id, create_uid, create_date, name, write_uid, notes, write_date, company) FROM stdin;
            coeficiente = CondoFactor(name = factor[3] if factor[3]!=pgnull else None,
                                      notes = factor[5] if factor[5]!=pgnull else None,
                                      )
            compania.condo_factors.append(coeficiente)

        units = filter(lambda x:x[4]==row[0], runit)
        for unit in sorted(units, key=lambda name: name[3]):
            #COPY condo_unit (id, create_uid, create_date, name, company, write_uid, write_date) FROM stdin;
            fraccion = CondoUnit(name = unit[3] if unit[3]!=pgnull else None)
            compania.condo_units.append(fraccion)

        compania.save()
        idcompany[row[0]] = compania.id

    else:
        print("Error: No se encuentra empresa " + row[0])

#set parent of companies
for row in rcompany:
    compania = get_company(row[0])

    if row[3]!=pgnull:
        if idcompany[row[3]]:
            padre = get_company(row[3])
            compania.parent = padre
            compania.save()
        else:
            print("Error: Verificar padre de la empresa " + row[0])

for row in rmandate:
    comunidad = get_company(row[3])
    tercero = get_party(row[10])
    accountnumber = get_bankaccountnumber(row[7])

    #COPY condo_payment_sepa_mandate (id, create_uid, create_date, company, write_uid, state, identification, account_number, write_date, signature_date, party, scheme, type) FROM stdin;
    condomandate = Mandate(company = comunidad,
                           signature_date = datetime.strptime(row[9],"%Y-%m-%d") if row[9]!=pgnull else None,
                           state = row[5] if row[5]!=pgnull else None,
                           identification = row[6] if row[6]!=pgnull else None,
                           account_number = accountnumber,
                           party = tercero,
                           scheme = row[11] if row[11]!=pgnull else None,
                           type = row[12] if row[12]!=pgnull else None)
    if accountnumber.account.owners and len(accountnumber.account.owners):
        condomandate.save()
    else:
        print("Error: numero de cuenta " + accountnumber.number \
            + " sin propietario y utilizada en mandato " + row[6] if row[6]!=pgnull else None \
            + " con estado " + row[5] if row[5]!=pgnull else None)
    idmandate[row[0]] = condomandate.id

for row in runit:

    fraccion = CondoUnit.find([('name', '=', row[3]), ('company','=', idcompany[row[4]])])

    if fraccion and len(fraccion)==1:
        unitfactors = filter(lambda x:x[7]==row[0], runitfactor)
        for unitfactor in sorted(unitfactors, key=lambda id: id[6]):
            condofactor, = filter(lambda x:x[0]==unitfactor[6], rfactor)
            comunidad = get_company(condofactor[7])
            coeficiente = CondoFactor.find([('name','=', condofactor[3]),('company','=', idcompany[condofactor[7]])])
            if coeficiente and len(coeficiente)==1:
                coeffrac = UnitFactor(value = Decimal(unitfactor[3]) if unitfactor!=pgnull else None,
                                      factor = coeficiente[0])
                fraccion[0].factors.append(coeffrac)
            else:
                print("Error: Coeficiente no encontrado para la fraccion " + row[0])

        condoparties = filter(lambda x:x[8]==row[0], rcondoparty)
        #COPY condo_party (id, create_uid, create_date, write_uid, role, write_date, active, party, unit, sepa_mandate, address, mail) FROM stdin;
        for condoparty in condoparties:
            direccion = None

            tercero = get_party(condoparty[7])

            if condoparty[10]!=pgnull:
                direccion = direcciones = None
                if condoparty[10] in idaddress:
                    direcciones = Address.find([('id', '=', idaddress[condoparty[10]])])
                    if (direcciones is None) or len(direcciones)==0:
                        direcciones = Address.find([('id', '=', idaddress[condoparty[10]]), ('active', '=', False)])
                if not direcciones or len(direcciones)!=1:
                    print("Error: direccion de correspondencia en propietario " + str(idparties[condoparty[7]]) + ' ' + tercero.name)
                else:
                    direccion = direcciones[0]

            mandato = None
            if condoparty[9]!=pgnull:
                mandato, = Mandate.find([('id', '=', idmandate[condoparty[9]])])

            party = CondoParty(role = condoparty[4] if condoparty[4]!=pgnull else None,
                               party = tercero,
                               address = direccion if condoparty[10]!=pgnull else None,
                               mail = False if (condoparty[11]=='f' or condoparty[11]==0) else True,
                               active = False if (condoparty[6]=='f' or condoparty[6]==0) else True,
                               sepa_mandate = mandato)
            fraccion[0].parties.append(party)

        fraccion[0].save()
    else:
        print("Error: No se encuentra fraccion " + row[0])

for row in sorted(rpain, key=lambda field:(field[5], idcompany[field[6]])):
    comunidad = get_company(row[6])
    #COPY condo_payment_pain (id, create_uid, sepa_receivable_flavor, create_date, write_date, reference, company, write_uid, state, message) FROM stdin;
    pain = CondoPain(sepa_receivable_flavor = row[2] if row[2]!=pgnull else None,
                     reference = row[5] if row[5]!=pgnull else None,
                     company = comunidad,
                     message = row[9].replace('\\r\\n', '\n').replace('\\n', '\n') if row[9]!=pgnull else None,
                     state = row[8] if row[8]!=pgnull else None)
    pain.save()
    idpains[row[0]] = pain.id

for row in sorted(rpaymentgroup, key=lambda field: (field[4], idcompany[field[5]])):

    #COPY condo_payment_group (id, create_uid, create_date, write_date, reference, company, write_uid, sepa_batch_booking, account_number, sepa_charge_bearer, date, pain, message) FROM stdin;
    comunidad = get_company(row[5])

    fact = None
    if row[11]!=pgnull: #case condo_payment_group is included in any pain message
        pain = CondoPain.find([('id', '=', idpains[row[11]])])
        if pain and len(pain)==1:
            fact = pain[0]
        else:
            print("Error: Facturacion referencia " + row[4] + " de comunidad " + comunidad.party.name)

    accountnumber = get_bankaccountnumber(row[8])

    #COPY condo_payment_group (id, create_uid, create_date, write_date, reference, company, write_uid, sepa_batch_booking, account_number, sepa_charge_bearer, date, pain, message) FROM stdin;
    grupo = CondoPaymentGroup(reference = row[4] if row[4]!=pgnull else None,
                              company = comunidad,
                              sepa_batch_booking = False if (row[7]=='f' or row[7]==0) else True,
                              account_number = accountnumber,
                              sepa_charge_bearer = row[9] if row[9]!=pgnull else None,
                              date = datetime.strptime(row[10],"%Y-%m-%d") if row[10]!=pgnull else None,
                              message = row[12].replace('\\r\\n', '\n').replace('\\n', '\n') if row[12]!=pgnull else None,
                              pain = fact)

    payments = filter(lambda x:x[12]==row[0], rpayment)
    #COPY condo_payment (id, create_uid, create_date, description, state, sepa_end_to_end_id, currency, amount, sepa_mandate, write_date, party, date, "group", write_uid, type, unit) FROM stdin;
    for payment in payments:

        moneda = get_currency(payment[6])

        fraccion = None
        if payment[15]!=pgnull:
            unit, = filter(lambda x:x[0]==payment[15], runit)
            #COPY condo_unit (id, create_uid, create_date, name, company, write_uid, write_date) FROM stdin;
            fraccion, = CondoUnit.find([('name', '=', unit[3]), ('company','=', idcompany[unit[4]])])

        tercero = get_party(payment[10])

        mandato = None
        if payment[8]!=pgnull:
            mandato, = Mandate.find([('id', '=', idmandate[payment[8]])])

        recibo = CondoPayment(currency = moneda,
                              unit = fraccion,
                              sepa_end_to_end_id = payment[5] if payment[5]!=pgnull else None,
                              state = payment[4] if payment[4]!=pgnull else None,
                              party = tercero,
                              type = payment[14] if payment[14]!=pgnull else None,
                              description = payment[3] if payment[3]!=pgnull else None,
                              sepa_mandate = mandato,
                              date = datetime.strptime(payment[11],"%Y-%m-%d") if payment[11]!=pgnull else None,
                              amount = Decimal(payment[7]) if payment[7]!=pgnull else None)
        grupo.payments.append(recibo)

    grupo.save()

for row in set([(r[2], r[4], r[8]) for r in sorted(riruiview, key=lambda field: (field[8],field[4]))]):
    users = User.find([('login', '!=', 'root'),
                       ('login', '!=', 'user_cron_trigger'),
                       ('active', '=', True)])
    for user in users:
        view = ViewSearch(
                          domain = row[0].decode('unicode-escape'),
                          name = row[1],
                          user = user,
                          model = row[2],
                          )
        view.save()
