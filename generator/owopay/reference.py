"""Reference data: the customers, merchants and agents every seeded day draws from.

Built from the seed alone, never from the day, so every learner has the same 50,000 customers.
"""

import datetime as dt
import random

N_CUSTOMERS = 50_000
N_MERCHANTS = 3_000
N_AGENTS = 1_500

WAT = dt.timezone(dt.timedelta(hours=1), 'WAT')

# Share of customers by state; Lagos and the FCT dominate, as they do for real fintechs.
STATES = {
    'Lagos': 30,
    'FCT': 10,
    'Rivers': 7,
    'Kano': 7,
    'Oyo': 6,
    'Ogun': 5,
    'Kaduna': 4,
    'Enugu': 4,
    'Anambra': 4,
    'Delta': 4,
    'Edo': 3,
    'Kwara': 3,
    'Abia': 3,
    'Akwa Ibom': 3,
    'Plateau': 2,
    'Ondo': 2,
    'Osun': 2,
    'Borno': 1,
}

FIRST_NAMES = [
    'Adaeze',
    'Chinedu',
    'Ngozi',
    'Emeka',
    'Ifeoma',
    'Obinna',
    'Chiamaka',
    'Uchenna',
    'Amaka',
    'Kelechi',
    'Oluwaseun',
    'Adebayo',
    'Funmilayo',
    'Tunde',
    'Yetunde',
    'Babajide',
    'Folake',
    'Damilola',
    'Kehinde',
    'Bisola',
    'Aisha',
    'Musa',
    'Fatima',
    'Ibrahim',
    'Zainab',
    'Abubakar',
    'Hauwa',
    'Sani',
    'Hadiza',
    'Usman',
    'Efe',
    'Oghenekaro',
    'Ese',
    'Tamuno',
    'Ebiere',
    'Osaze',
    'Nosa',
    'Ekaette',
    'Ime',
    'Bassey',
]
LAST_NAMES = [
    'Okafor',
    'Eze',
    'Nwosu',
    'Okonkwo',
    'Obi',
    'Chukwu',
    'Adeyemi',
    'Ogunleye',
    'Balogun',
    'Afolabi',
    'Bello',
    'Abdullahi',
    'Yusuf',
    'Mohammed',
    'Danjuma',
    'Okoro',
    'Etim',
    'Akpan',
    'Briggs',
    'Ighalo',
    'Oyelaran',
    'Adesanya',
    'Ibekwe',
    'Lawal',
    'Olatunji',
    'Garba',
    'Umeh',
    'Ekwueme',
    'Omotola',
    'Edet',
]
PHONE_PREFIXES = [
    '803',
    '806',
    '813',
    '816',
    '703',
    '706',
    '802',
    '808',
    '812',
    '701',
    '805',
    '807',
    '815',
    '809',
    '817',
    '818',
    '909',
    '908',
    '901',
    '902',
]
SIGNUP_CHANNELS = {'app': 70, 'ussd': 20, 'agent': 10}
KYC_TIERS = {1: 55, 2: 35, 3: 10}

MERCHANT_CATEGORIES = {
    'groceries': 20,
    'food_and_drink': 18,
    'airtime_and_data': 12,
    'transport': 10,
    'fashion': 8,
    'pharmacy': 7,
    'electronics': 6,
    'bills_and_utilities': 8,
    'education': 5,
    'betting': 6,
}
MERCHANT_WORDS = [
    'Mama',
    'Royal',
    'Divine',
    'Golden',
    'Eko',
    'Naija',
    'Unity',
    'Grace',
    'Prime',
    'Favour',
    'Mega',
    'City',
    'Star',
    'Zion',
    'Ola',
    'Ade',
    'Chi',
    'Bright',
    'Express',
    'Hub',
]
MERCHANT_SUFFIX = {
    'groceries': 'Stores',
    'food_and_drink': 'Kitchen',
    'airtime_and_data': 'Telecoms',
    'transport': 'Motors',
    'fashion': 'Fabrics',
    'pharmacy': 'Pharmacy',
    'electronics': 'Gadgets',
    'bills_and_utilities': 'Payments',
    'education': 'Academy',
    'betting': 'Bets',
}

FIRST_SIGNUP = dt.datetime(2024, 1, 1, tzinfo=WAT)
LAST_SIGNUP = dt.datetime(2026, 10, 31, tzinfo=WAT)


def _weighted(rng, table):
    return rng.choices(list(table), weights=list(table.values()))[0]


def _signup_time(rng):
    span = (LAST_SIGNUP - FIRST_SIGNUP).total_seconds()
    return FIRST_SIGNUP + dt.timedelta(seconds=int(rng.random() * span))


def customers(seed):
    rng = random.Random(seed * 7 + 1)
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        created = _signup_time(rng)
        rows.append(
            {
                'customer_id': 'CUS-%06d' % i,
                'full_name': '%s %s' % (rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)),
                'phone_number': '+234%s%07d' % (rng.choice(PHONE_PREFIXES), rng.randrange(10_000_000)),
                'bvn': '22%09d' % rng.randrange(1_000_000_000),
                'state': _weighted(rng, STATES),
                'kyc_tier': _weighted(rng, KYC_TIERS),
                'signup_channel': _weighted(rng, SIGNUP_CHANNELS),
                'created_at': created,
                'updated_at': created,
            }
        )
    return rows


def merchants(seed):
    rng = random.Random(seed * 7 + 2)
    rows = []
    for i in range(1, N_MERCHANTS + 1):
        category = _weighted(rng, MERCHANT_CATEGORIES)
        rows.append(
            {
                'merchant_id': 'MER-%05d' % i,
                'business_name': '%s %s %s'
                % (rng.choice(MERCHANT_WORDS), rng.choice(MERCHANT_WORDS), MERCHANT_SUFFIX[category]),
                'category': category,
                'state': _weighted(rng, STATES),
                'created_at': _signup_time(rng),
            }
        )
    return rows


def agents(seed):
    rng = random.Random(seed * 7 + 3)
    rows = []
    for i in range(1, N_AGENTS + 1):
        rows.append(
            {
                'agent_id': 'AGT-%05d' % i,
                'business_name': '%s %s POS' % (rng.choice(MERCHANT_WORDS), rng.choice(LAST_NAMES)),
                'state': _weighted(rng, STATES),
                'created_at': _signup_time(rng),
            }
        )
    return rows


def wallets(seed, customer_rows):
    rng = random.Random(seed * 7 + 4)
    return [
        {
            'wallet_id': 'WAL-%06d' % i,
            'customer_id': c['customer_id'],
            'balance_kobo': int(rng.lognormvariate(10.5, 1.4)) * 100,
            'currency': 'NGN',
            'updated_at': c['created_at'],
        }
        for i, c in enumerate(customer_rows, start=1)
    ]
