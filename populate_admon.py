#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

from proteus import config, Model

import os
import unicodecsv
from cStringIO import StringIO
from decimal import Decimal
from datetime import datetime

config = config.set_trytond('sqlite://')

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
Currency = Model.get('currency.currency')
Identifier = Model.get('party.identifier')
Lang = Model.get('ir.lang')
Mandate = Model.get('condo.payment.sepa.mandate')
Party = Model.get('party.party')
PartyRelation = Model.get('party.relation.all')
PartyRelationType = Model.get('party.relation.type')
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

for row in sorted(rcategory, key=lambda field: field[0]):
    padre = None
    if row[4]!='\N':
        if idcategor[row[4]]:
            padre, = Category.find([('id', '=', idcategor[row[4]])])
        else:
            print "Error: Verificar padre de la categoria " + row[0]

    category = Category.find([('name', '=', row[3])])
    if len(category)==0:
        category = Category.find([('name', '=', row[3]), ('active', '=', False)])
        if len(category)==0:
            #COPY party_category (id, create_uid, create_date, name, parent, write_uid, write_date, active) FROM stdin;
            categoria = Category(name = row[3],
                                 parent = padre,
                                 active = False if (row[7]=='f' or row[7]==0) else True)
            categoria.save()
            idcategor[row[0]] = categoria.id
        elif len(category)==1:
            idcategor[row[0]] = category[0].id

    elif len(category)==1:
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
                                        ('src', '=', translation[2] if translation[2]!='\N' else None),
                                        ('name', '=', translation[4] if translation[4]!='\N' else None),
                                        ('res_id', '=', translation[5] if translation[5]!='\N' else None),
                                        ('type', '=', translation[7] if translation[7]!='\N' else None),
                                        ('module', '=', translation[8] if translation[8]!='\N' else None)
                                      ])

        if len(traduccion):
            traduccion[0].lang = translation[1]
            traduccion[0].value = translation[6]
            traduccion[0].save()
        else:
            #TODO?
            traduccion = Translation(lang = translation[1],
                                     src = translation[2],
                                     name = translation[4] if translation[4]!='\N' else None,
                                     res_id = translation[5] if translation[5]!='\N' else None,
                                     value = translation[6],
                                     type = translation[7] if translation[7]!='\N' else None,
                                     module = translation[8] if translation[8]!='\N' else None)
            traduccion.save()


for row in rreltype:
    if row[3]!='\N':
        reverse, = PartyRelationType.find([('id', '=', idpreltyp[row[3]])])
        reltype, = PartyRelationType.find([('id', '=', idpreltyp[row[0]])])
        reltype.reverse = reverse
        reltype.save()

for row in raccount:

    moneda = None
    if row[4]:
        moneda, = Currency.find([('id', '=', row[4])])

    banco, = Bank.find([('id', '=', row[7])])
    #COPY bank_account (id, create_uid, create_date, write_uid, currency, write_date, active, bank) FROM stdin;
    account = BankAccount(currency = moneda,
                          active = False if (row[6]=='f' or row[6]==0) else True,
                          bank = banco)
    accountnumbers = filter(lambda x:x[2]==row[0], raccountnumber)
    #COPY bank_account_number (id, create_uid, account, create_date, sequence, number, write_uid, write_date, number_compact, type) FROM stdin;
    for accountnumber in accountnumbers:
        account.numbers.new(type = accountnumber[9],
                            number = accountnumber[5])
    account.save()
    idcuentas[row[0]] = account.id

for row in sorted(rparty, key=lambda field:(False if field[0] in [r[10] for r in rcompany] else True, field[7])):

    #Banks has id<=420
    if Decimal(row[0])<=420:
        continue

#TODO lang
#        if row[]!='\N':
#            lenguaje, = Lang.find([('id', '=', 'es_ES')])

    #COPY party_party (id, code, create_date, write_uid, create_uid, write_date, active, name) FROM stdin;
    party = Party(name = row[7], 
                  active = False if (row[6]=='f' or row[6]==0) else True,
                  lang = es)

    addresses = filter(lambda x:x[14]==row[0], raddress)
    #COPY party_address (id, city, create_date, name, zip, create_uid, country, sequence, subdivision, write_uid, streetbis, street, write_date, active, party) FROM stdin;
    for address, i in zip(addresses, range(len(addresses))):
        provincia = pais = None
        if address[8]!='\N':
            provincia, = Subdivision.find([('id', '=', address[8])])
        if address[6]!='\N':
            pais, = Country.find([('id', '=', address[6])])
        if i!=0:
            direccion = Address(name = address[3] if address[3]!='\N' else None,
                                street = address[11] if address[11]!='\N' else None,
                                streetbis = address[10] if address[10]!='\N' else None,
                                zip = address[4] if address[4]!='\N' else None,
                                city = address[1] if address[1]!='\N' else None,
                                subdivision = provincia,
                                country = pais,
                                active = False if (address[13]=='f' or address[13]==0) else True)
            party.addresses.append(direccion)

        else:
            party.addresses[0].name = address[3] if address[3]!='\N' else None
            party.addresses[0].street = address[11] if address[11]!='\N' else None
            party.addresses[0].zip = address[4] if address[4]!='\N' else None
            party.addresses[0].city = address[1] if address[1]!='\N' else None
            party.addresses[0].subdivision = provincia
            party.addresses[0].country = pais
            party.addresses[0].active = False if (address[13]=='f' or address[13]==0) else True

    contacts = filter(lambda x:x[9]==row[0], rcontact)
    for contact in contacts:
        #COPY party_contact_mechanism (id, comment, create_uid, create_date, sequence, value, write_uid, write_date, active, party, type) FROM stdin;
        contacto = ContactMechanism(comment = contact[1] if contact[1]!='\N' else None,
                                    value = contact[5] if contact[5]!='\N' else None,
                                    type = contact[10] if contact[10]!='\N' else None,
                                    active = False if (contact[8]=='f' or contact[8]==0) else True)
        party.contact_mechanisms.append(contacto)

    accounts = filter(lambda x:x[6]==row[0], raccountnumberparty)
    seen = set()
    for account in accounts:
        if account[2] in seen:
            print "Error: Cuenta y propietarios repetidos " + account[2]
            continue
        else:
            seen.add(account[2])

        cuenta = None
        if account[2]!='\N' and idcuentas[account[2]]:
            cuenta = BankAccount.find([('id', '=', idcuentas[account[2]])])
            if len(cuenta)==1:
                party.bank_accounts.append(cuenta[0])
            elif len(cuenta)==0:
                cuenta = BankAccount.find([('id', '=', idcuentas[account[2]]), ('active', '=', False)])
                if len(cuenta)==1:
                    party.bank_accounts.append(cuenta[0])
                else:
                    print "Error: Cuenta sin propietario " + account[2]
        else:
            print "Error: Verificar cuenta bancaria " + account[2]

    identifiers = filter(lambda x:x[6]==row[0], ridentifier)
    for identifier in identifiers:
        #COPY party_identifier (id, create_uid, code, create_date, write_uid, write_date, party, type) FROM stdin;
        identificacion = Identifier(code = identifier[2] if identifier[2]!='\N' else None,
                                    type = identifier[7] if identifier[7]!='\N' else None)
        party.identifiers.append(identificacion)

    categories = filter(lambda x:x[6]==row[0], rcategoryrel)
    for category in categories:
        categoria, = Category.find([('id', '=', idcategor[category[1]])])
        party.categories.append(categoria)

    party.save()
    idparties[row[0]] = party.id

    addresses = filter(lambda x:x[14]==row[0], raddress)
    for address, i in zip(addresses, range(len(party.addresses))):
            idaddress[address[0]] = party.addresses[i].id

for row in rrelation:
    tercero_to = tercero_from = None
    tercero_to = Party.find([('id', '=', idparties[row[4]])])
    if len(tercero_to)==0:
        tercero_to = Party.find([('id', '=', idparties[row[4]]), ('active', '=', False)])

    tercero_from = Party.find([('id', '=', idparties[row[6]])])
    if len(tercero_from)==0:
        tercero_from = Party.find([('id', '=', idparties[row[6]]), ('active', '=', False)])

    tipo, = PartyRelationType.find([('id', '=', idpreltyp[row[7]])])

    relation = PartyRelation(to = tercero_to[0],
                             from_ = tercero_from[0],
                             type = tipo)
    relation.save()

#note: package pytz must be installed on server (otherwise comment out timezone field attribution)
for row in sorted(rcompany, key=lambda field: filter(lambda x:x[0]==field[10], rparty)[0][7]):
    moneda = padre = None
    if row[7]!='\N':
        moneda, = Currency.find([('id', '=', row[7])])

    tercero = Party.find([('id', '=', idparties[row[10]])])

    if len(tercero)==0:
        tercero = Party.find([('id', '=', idparties[row[10]]), ('active', '=', False)])

    if len(tercero)==1:
        compania = Company(parent = padre,
                           footer = row[4] if row[4]!='\N' else None,
                           header = row[5] if row[5]!='\N' else None,
                           currency = moneda,
                           timezone = row[9] if row[9]!='\N' else None,
                           party = tercero[0],
                           is_Condominium = False if (row[11]=='f' or row[11]==0) else True,
                           creditor_bussines_code = row[12] if row[12]!='\N' else None,
                           sepa_creditor_identifier = row[13] if row[13]!='\N' else None)

        factors = filter(lambda x:x[8]==row[0], rfactor)
        for factor in factors:
            #COPY condo_factor (id, create_uid, create_date, name, write_uid, notes, write_date, total, company) FROM stdin;
            coeficiente = CondoFactor(name = factor[3] if factor[3]!='\N' else None,
                                      notes = factor[5] if factor[5]!='\N' else None,
                                      total = Decimal(factor[7]) if factor[7]!='\N' else None)
            compania.condo_factors.append(coeficiente)

        units = filter(lambda x:x[4]==row[0], runit)
        for unit in sorted(units, key=lambda name: name[3]):
            #COPY condo_unit (id, create_uid, create_date, name, company, write_uid, write_date) FROM stdin;
            fraccion = CondoUnit(name = unit[3] if unit[3]!='\N' else None)
            compania.condo_units.append(fraccion)

        compania.save()
        idcompany[row[0]] = compania.id

    else:
        print "Error: No se encuentra empresa " + row[0]

#set parent of companies
for row in rcompany:
    compania, = Company.find([('id', '=', idcompany[row[0]])])

    if row[3]!='\N':
        if idcompany[row[3]]:
            padre, = Company.find([('id', '=', idcompany[row[3]])])
            compania.parent = padre
            compania.save()
        else:
            print "Error: Verificar padre de la empresa " + row[0]

for row in rmandate:
    comunidad = Company.find([('id', '=', idcompany[row[3]])])

    tercero = Party.find([('id', '=', idparties[row[10]])])
    if len(tercero)==0:
        tercero = Party.find([('id', '=', idparties[row[10]]), ('active', '=', False)])

    accountnumber = None
    accountnumbers = filter(lambda x:x[0]==row[7], raccountnumber)
    if len(accountnumbers)==1:
        accountnumber, = BankAccountNumber.find([('number', '=', accountnumbers[0][5])])
    else:
        print "Error: numero de cuenta " + row[7]
    #COPY condo_payment_sepa_mandate (id, create_uid, create_date, company, write_uid, state, identification, account_number, write_date, signature_date, party, scheme, type) FROM stdin;
    condomandate = Mandate(company = comunidad[0],
                           signature_date = datetime.strptime(row[9],"%Y-%m-%d") if row[9]!='\N' else None,
                           state = row[5] if row[5]!='\N' else None,
                           identification = row[6] if row[6]!='\N' else None,
                           account_number = accountnumber,
                           party = tercero[0],
                           scheme = row[11] if row[11]!='\N' else None,
                           type = row[12] if row[12]!='\N' else None)
    condomandate.save()
    idmandate[row[0]] = condomandate.id

for row in runit:

    fraccion = CondoUnit.find([('name', '=', row[3]), ('company','=', idcompany[row[4]])])

    if len(fraccion)==1:
        unitfactors = filter(lambda x:x[7]==row[0], runitfactor)
        for unitfactor in sorted(unitfactors, key=lambda id: id[6]):
            condofactor, = filter(lambda x:x[0]==unitfactor[6], rfactor)
            comunidad, = Company.find([('id','=', idcompany[condofactor[8]])])
            coeficiente = CondoFactor.find([('name','=', condofactor[3]),('company','=', idcompany[condofactor[8]])])
            if len(coeficiente)==1:
                coeffrac = UnitFactor(value = Decimal(unitfactor[3]) if unitfactor!='\N' else None,
                                      factor = coeficiente[0])
                fraccion[0].factors.append(coeffrac)
            else:
                print "Error: Coeficiente no encontrado para la fraccion " + row[0]

        condoparties = filter(lambda x:x[4]==row[0], rcondoparty)
        #COPY condo_party (id, create_uid, create_date, write_uid, unit, role, party, write_date, address, mail, active, sepa_mandate) FROM stdin;
        for condoparty in condoparties:
            direccion = None

            tercero = Party.find([('id', '=', idparties[condoparty[6]])])
            if len(tercero)==0:
                tercero = Party.find([('id', '=', idparties[condoparty[6]]), ('active', '=', False)])

            if condoparty[8]!='\N':
                direccion, = Address.find([('id', '=', idaddress[condoparty[8]])])

            mandato = None
            if condoparty[11]!='\N':
                mandato, = Mandate.find([('id', '=', idmandate[condoparty[11]])])

            party = CondoParty(role = condoparty[5] if condoparty[5]!='\N' else None,
                               party = tercero[0],
                               address = direccion,
                               mail = False if (condoparty[9]=='f' or condoparty[9]==0) else True,
                               active = False if (condoparty[10]=='f' or condoparty[10]==0) else True,
                               sepa_mandate = mandato)
            fraccion[0].parties.append(party)

        fraccion[0].save()
    else:
        print "Error: No se encuentra fraccion " + row[0]

for row in sorted(rpain, key=lambda field:(field[5], idcompany[field[6]])):
    comunidad = Company.find([('id', '=', idcompany[row[6]])])
    #COPY condo_payment_pain (id, create_uid, sepa_receivable_flavor, create_date, write_date, reference, company, write_uid, state, message) FROM stdin;
    pain = CondoPain(sepa_receivable_flavor = row[2] if row[2]!='\N' else None,
                     reference = row[5] if row[5]!='\N' else None,
                     company = comunidad[0],
                     message = row[9].replace('\\r\\n', '\n').replace('\\n', '\n') if row[9]!='\N' else None,
                     state = row[8] if row[8]!='\N' else None)
    pain.save()
    idpains[row[0]] = pain.id

for row in sorted(rpaymentgroup, key=lambda field: (field[4], idcompany[field[5]])):

    #COPY condo_payment_group (id, create_uid, create_date, write_date, reference, company, write_uid, sepa_batch_booking, account_number, sepa_charge_bearer, date, message, pain) FROM stdin;
    comunidad2 = Company.find([('id', '=', idcompany[row[5]])])
    
    fact = None
    pain = CondoPain.find([('id', '=', idpains[row[12]])])
    if len(pain)==1:
        fact = pain[0]

    accountnumber = None
    accountnumbers = filter(lambda x:x[0]==row[8], raccountnumber)
    if len(accountnumbers)==1:
        accountnumber, = BankAccountNumber.find([('number', '=', accountnumbers[0][5])])
    else:
        print "Error: numero de cuenta " + row[8]

    #COPY condo_payment_group (id, create_uid, create_date, write_date, reference, company, write_uid, sepa_batch_booking, account_number, sepa_charge_bearer, date, message, pain) FROM stdin;
    grupo = CondoPaymentGroup(reference = row[4] if row[4]!='\N' else None,
                              company = comunidad2[0],
                              sepa_batch_booking = False if (row[7]=='f' or row[7]==0) else True,
                              account_number = accountnumber,
                              sepa_charge_bearer = row[9] if row[9]!='\N' else None,
                              date = datetime.strptime(row[10],"%Y-%m-%d") if row[10]!='\N' else None,
                              message = row[11].replace('\\r\\n', '\n').replace('\\n', '\n') if row[11]!='\N' else None,
                              pain = fact)

    payments = filter(lambda x:x[7]==row[0], rpayment)
    #COPY condo_payment (id, create_date, write_uid, currency, unit, create_uid, fee, "group", sepa_end_to_end_id, state, party, type, description, sepa_mandate, write_date, date, amount) FROM stdin;
    for payment in payments:

        moneda = None
        if payment[3]!='\N':
            moneda, = Currency.find([('id', '=', payment[3])])

        fraccion = None
        if payment[4]!='\N':
            unit, = filter(lambda x:x[0]==payment[4], runit)
            #COPY condo_unit (id, create_uid, create_date, name, company, write_uid, write_date) FROM stdin;
            fraccion, = CondoUnit.find([('name', '=', unit[3]), ('company','=', idcompany[unit[4]])])

        tercero = None
        if payment[10]!='\N':
            cparty = Party.find([('id', '=', idparties[payment[10]])])
            if len(cparty)==0:
                tercero, = Party.find([('id', '=', idparties[payment[10]]), ('active', '=', False)])
            elif len(cparty)==1:
                tercero = cparty[0]

        mandato = None
        if payment[13]!='\N':
            mandato, = Mandate.find([('id', '=', idmandate[payment[13]])])

        recibo = CondoPayment(currency = moneda,
                              unit = fraccion,
                              fee = False if (payment[6]=='f' or payment[6]==0) else True,
                              sepa_end_to_end_id = payment[8] if payment[8]!='\N' else None,
                              state = payment[9] if payment[9]!='\N' else None,
                              party = tercero,
                              type = payment[11] if payment[11]!='\N' else None,
                              description = payment[12] if payment[12]!='\N' else None,
                              sepa_mandate = mandato,
                              date = datetime.strptime(payment[15],"%Y-%m-%d") if payment[15]!='\N' else None,
                              amount = Decimal(payment[16]) if payment[16]!='\N' else None)
        grupo.payments.append(recibo)

    grupo.save()

for row in set([(r[2], r[4], r[8]) for r in sorted(riruiview, key=lambda field: (field[8],field[4]))]):
    users = User.find([('login', '!=', 'root'),
                       ('login', '!=', 'user_cron_trigger'),
                       ('active', '=', True)])
    for user in users:
        view = ViewSearch(domain = row[0].decode('unicode-escape'),
                          name = row[1],
                          user = user,
                          model = row[2])
        view.save()
