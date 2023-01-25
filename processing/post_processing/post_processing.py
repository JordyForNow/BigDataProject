import pandas as pd


def post_process(df: pd.DataFrame) -> pd.DataFrame:
    df = simple_process(df)
    df = host_process(df)
    return df.groupby('host', dropna=False).sum().reset_index()


def simple_process(df: pd.DataFrame) -> pd.DataFrame:
    host = df['host']

    # de-amp
    amp_selector = host.str.endswith('cdn.ampproject.org', na=False)
    amp_df = df.loc[amp_selector, 'host']
    amp_df = amp_df.str.removeprefix('www-')
    amp_df = amp_df.str.removesuffix('.cdn.ampproject.org')
    # use / as placeholder
    amp_df = amp_df.str.replace('--', '/')
    amp_df = amp_df.str.replace('-', '.')
    amp_df = amp_df.str.replace('/', '-')
    df.loc[amp_selector, 'host'] = amp_df
    # Make all lowercase
    host = host.str.lower()
    # Remove '.' from the end if present
    host = host.str.removesuffix('.')
    # Remove 'www.' from start if present
    host = host.str.removeprefix('www.')

    df['host'] = host

    return df


def host_process(df: pd.DataFrame) -> pd.DataFrame:
    host = df['host']

    def domain_selector(domain: str):
        return host.str.endswith(f'.{domain}', na=False) | (host == domain)

    # google search
    google_selector = host.str.startswith('google.', na=False) | host.isin([
        'com.google.android.googlequicksearchbox',
        'startgoogle.startpagina.nl'
    ])
    df.loc[google_selector, 'host'] = 'google.com'

    # google plus
    google_plus_selector = (host == 'plus.google.com') | (host == 'plus.url.google.com')
    df.loc[google_plus_selector, 'host'] = 'plus.google.com'

    # twitter
    twitter_selector = host.str.endswith('.twitter.com', na=False) | (host == 't.co') | (host == 'twitter.com')
    df.loc[twitter_selector, 'host'] = 'twitter.com'

    # websdr

    # reverse_dns = {
    #     '192.87.173.88': 'etgd-websdr.ewi.utwente.nl',
    #     '130.89.192.8': 'etgd1-old.ewi.utwente.nl',
    #     '2001:67c:2564:532:250:daff:fe6e:c945': 'websdr.ewi.utwente.nl',
    # }

    websdr_selector = host.isin([
        '192.87.173.88',
        '130.89.192.8',
        '2001:67c:2564:532:250:daff:fe6e:c945',
        '2001:67c:2564:ac33:da5d:4cff:fe80:9a66',
        'etgd1.ewi.utwente.nl',
        'websdr.ewi.utwente.nl',
        'websdr.org',
        'websdr6',
        'etgd-websdr.ewi.utwente.nl'
    ])
    df.loc[websdr_selector, 'host'] = 'websdr.ewi.utwente.nl'

    # yahoo search
    yahoo_selector = host.str.endswith('search.yahoo.com', na=False) | host.str.endswith('search.yahoo.co.jp', na=False)
    df.loc[yahoo_selector, 'host'] = 'search.yahoo.com'

    # yandex
    yandex_selector = host.str.startswith('yandex.', na=False)
    df.loc[yandex_selector, 'host'] = 'yandex.ru'

    # telegram
    telegram_selector = host.str.startswith('org.telegram', na=False)
    df.loc[telegram_selector, 'host'] = 'org.telegram.messenger'

    # blogspot
    blogspot_selector = host.str.contains('.blogspot.', na=False)
    df.loc[blogspot_selector, 'host'] = df.loc[blogspot_selector, 'host'].apply(lambda x : x.split('.')[0] + '.blogspot.com')

    # instagram
    instagram_selector = host.str.endswith('instagram.com', na=False)
    df.loc[instagram_selector, 'host'] = 'instagram.com'

    # fbi
    fbi_selector = host.str.startswith('fbi.', na=False)
    df.loc[fbi_selector, 'host'] = 'fbi.com'

    # webjam
    webjam_selector = host.isin(['webjam.com', 'webjam2.com'])
    df.loc[webjam_selector, 'host'] = 'webjam.com'

    # cbforum
    cbforum_selector = host.str.startswith('cbforum.', na=False)
    df.loc[cbforum_selector, 'host'] = 'cbforum.nl'

    # radio mi amigo
    radiomiamigo_selector = host.isin(['radiomiamigo.international', 'radiomiamigointernational.com'])
    df.loc[radiomiamigo_selector, 'host'] = 'radiomiamigo.international'

    # gizmodo
    gizmodo_selector = host.str.contains('gizmodo', na=False) | (host == 'io9.com')
    df.loc[gizmodo_selector, 'host'] = 'io9.gizmodo.com'

    # ref union
    ref_selector = host.str.contains('ref-union', na=False) | host.str.contains('r-e-f', na=False)
    df.loc[ref_selector, 'host'] = 'ref-union.org'

    # burp collaborator
    burp_filter = host.str.contains('burpcollab', na=False)
    df.loc[burp_filter, 'host'] = 'burpcollaborator.net'

    # TODO add gmail/gmail app and other mail clients?

    # simple domains

    simple_domains = [
        'facebook.com',
        'reddit.com',
        'wikipedia.org',
        'youtube.com',
        'bing.com',
        'vk.com',
        'baidu.com',
        'duckduckgo.com',
        '4chan.org',
        'pikabu.ru',
        'qrz.ru',
        'commentcamarche.net',
        'darc.de',
        '9x.hk',
        'jeuxvideo.com',
        'startpage.com',
        'forocoches.com',
        'animalnewyork.com',
        'leowood.net',
        'so.com',
        'ok.ru',
        'ask.com',
        'vfdb.org',
        'tweakers.net',
        'taringa.net',
        'short-wave.info',
        'sogou.com'
    ]

    for simple_domain in simple_domains:
        df.loc[domain_selector(simple_domain), 'host'] = simple_domain

    return df
