# coding: utf-8
from datetime import datetime
import requests
from lxml import etree

URL = "https://www.bundestag.de/"
MDB_INDEX_URL = URL + "xml/mdb/index.xml"
AUSSCHUSS_INDEX_URL = URL + "xml/ausschuesse/index.xml"
AUSSCHUSS_PATTERN = URL + "bundestag/ausschuesse17/%s/index.jsp"


def open_xml(url):
    res = requests.get(url)
    return etree.parse(res.content)


def parse_date(text):
    return datetime.strptime(text, '%d.%m.%Y').isoformat()


def add_to_gremium(node, url, role, engine):
    key = node.get('id')
    table = sl.get_table(engine, 'gremium')
    g = sl.find_one(engine, table, key=key)
    if g is None:
        g = {'key': key, 'type': 'sonstiges'}
        g['name'] = node.findtext('gremiumName')
        g['url'] = node.findtext('gremiumURL')
        sl.upsert(engine, table, g, unique=['key'])
    table = sl.get_table(engine, 'gremium_mitglieder')
    sl.upsert(engine, table, {
        'gremium_key': g['key'],
        'person_source_url': url,
        'role': role
    }, unique=['person_source_url', 'gremium_key', 'role'])


def scrape_gremium(engine, url, force=False):
    doc = open_xml(url)
    a = {'source_url': url}
    a['key'] = doc.findtext('/ausschussId')
    a['name'] = doc.findtext('/ausschussName')
    a['aufgabe'] = doc.findtext('/ausschussAufgabe')
    a['image_url'] = doc.findtext('/ausschussBildURL')
    a['image_copyright'] = doc.findtext('/ausschussCopyright')
    a['url'] = AUSSCHUSS_PATTERN % a['key']
    a['type'] = 'ausschuss'
    return a


def scrape_index():
    doc = open_xml(AUSSCHUSS_INDEX_URL)
    for info_url in doc.findall("//ausschussDetailXML"):
        yield info_url.text.strip()

    doc = open_xml(MDB_INDEX_URL)
    for info_url in doc.findall("//mdbInfoXMLURL"):
        yield info_url.text


def scrape_mdb(engine, url, force=False):
    doc = open_xml(url)
    id = int(doc.findtext('//mdbID'))
    table_person = sl.get_table(engine, 'person')
    table_rolle = sl.get_table(engine, 'rolle')
    p = {'source_url': url}
    r = {'person_source_url': url, 'funktion': 'MdB'}

    r['mdb_id'] = p['mdb_id'] = id
    r['status'] = doc.find('//mdbID').get('status')
    if doc.findtext('//mdbAustrittsdatum'):
        r['austritt'] = parse_date(doc.findtext('//mdbAustrittsdatum'))
    p['vorname'] = doc.findtext('//mdbVorname')
    p['nachname'] = doc.findtext('//mdbZuname')
    p['adelstitel'] = doc.findtext('//mdbAdelstitel')
    p['titel'] = doc.findtext('//mdbAkademischerTitel')
    p['ort'] = doc.findtext('//mdbOrtszusatz')
    p['geburtsdatum'] = doc.findtext('//mdbGeburtsdatum')
    p['religion'] = doc.findtext('//mdbReligionKonfession')
    p['hochschule'] = doc.findtext('//mdbHochschulbildung')
    p['beruf'] = doc.findtext('//mdbBeruf')
    p['berufsfeld'] = doc.find('//mdbBeruf').get('berufsfeld')
    p['geschlecht'] = doc.findtext('//mdbGeschlecht')
    p['familienstand'] = doc.findtext('//mdbFamilienstand')
    p['kinder'] = doc.findtext('//mdbAnzahlKinder')
    r['fraktion'] = doc.findtext('//mdbFraktion')
    p['fraktion'] = doc.findtext('//mdbFraktion')
    p['partei'] = doc.findtext('//mdbPartei')
    p['land'] = doc.findtext('//mdbLand')
    r['gewaehlt'] = doc.findtext('//mdbGewaehlt')
    p['bio_url'] = doc.findtext('//mdbBioURL')
    p['bio'] = doc.findtext('//mdbBiografischeInformationen')
    p['wissenswertes'] = doc.findtext('//mdbWissenswertes')
    p['homepage_url'] = doc.findtext('//mdbHomepageURL')
    p['telefon'] = doc.findtext('//mdbTelefon')
    p['angaben'] = doc.findtext('//mdbVeroeffentlichungspflichtigeAngaben')
    p['foto_url'] = doc.findtext('//mdbFotoURL')
    p['foto_copyright'] = doc.findtext('//mdbFotoCopyright')
    p['reden_plenum_url'] = doc.findtext('//mdbRedenVorPlenumURL')
    p['reden_plenum_rss_url'] = doc.findtext('//mdbRedenVorPlenumRSS')

    p['wk_nummer'] = doc.findtext('//mdbWahlkreisNummer')
    p['wk_name'] = doc.findtext('//mdbWahlkreisName') 
    p['wk_url'] = doc.findtext('//mdbWahlkreisURL')

    for website in doc.findall('//mdbSonstigeWebsite'):
        type_ = website.findtext('mdbSonstigeWebsiteTitel')
        ws_url = website.findtext('mdbSonstigeWebsiteURL')
        if type_.lower() == 'twitter':
            p['twitter_url'] = ws_url
        if type_.lower() == 'facebook':
            p['facebook_url'] = ws_url

    if doc.findtext('//mdbBundestagspraesident'):
        sl.upsert(engine, table_rolle, {
            'person_source_url': url,
            'funktion': u'Bundestagspräsident',
            },
            unique=['person_source_url', 'funktion'])
    if doc.findtext('//mdbBundestagsvizepraesident'):
        sl.upsert(engine, table_rolle, {
            'person_source_url': url,
            'funktion': u'Bundestagsvizepräsident',
            },
            unique=['person_source_url', 'funktion'])

    for n in doc.findall('//mdbObleuteGremium'):
        add_to_gremium(n, url, 'obleute', engine)

    for n in doc.findall('//mdbVorsitzGremium'):
        add_to_gremium(n, url, 'vorsitz', engine)

    for n in doc.findall('//mdbStellvertretenderVorsitzGremium'):
        add_to_gremium(n, url, 'stellv_vorsitz', engine)

    for n in doc.findall('//mdbVorsitzSonstigesGremium'):
        add_to_gremium(n, url, 'vorsitz', engine)

    for n in doc.findall('//mdbStellvVorsitzSonstigesGremium'):
        add_to_gremium(n, url, 'stellv_vorsitz', engine)

    for n in doc.findall('//mdbOrdentlichesMitgliedGremium'):
        add_to_gremium(n, url, 'mitglied', engine)

    for n in doc.findall('//mdbStellvertretendesMitgliedGremium'):
        add_to_gremium(n, url, 'stellv_mitglied', engine)

    for n in doc.findall('//mdbOrdentlichesMitgliedSonstigesGremium'):
        add_to_gremium(n, url, 'mitglied', engine)

    for n in doc.findall('//mdbStellvertretendesMitgliedSonstigesGremium'):
        add_to_gremium(n, url, 'stellv_mitglied', engine)

    sl.upsert(engine, table_person, p, unique=['source_url'])
    sl.upsert(engine, table_rolle, r, unique=['person_source_url', 'funktion'])
    return p
