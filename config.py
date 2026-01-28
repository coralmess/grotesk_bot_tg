import os
from dotenv import load_dotenv
import random

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
EXCHANGERATE_API_KEY = os.getenv('EXCHANGERATE_API_KEY')
DANYLO_DEFAULT_CHAT_ID = os.getenv('DANYLO_DEFAULT_CHAT_ID')
TELEGRAM_OLX_BOT_TOKEN = os.getenv('TELEGRAM_OLX_BOT_TOKEN')
IS_RUNNING_LYST = os.getenv('IsRunningLyst', 'true').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
CHECK_INTERVAL_SEC = int(os.getenv('CHECK_INTERVAL_SEC', '3600'))
CHECK_JITTER_SEC = int(os.getenv('CHECK_JITTER_SEC', '300'))
OLX_REQUEST_JITTER_SEC = float(os.getenv('OLX_REQUEST_JITTER_SEC', '2.0'))
SHAFA_REQUEST_JITTER_SEC = float(os.getenv('SHAFA_REQUEST_JITTER_SEC', '2.0'))
MAINTENANCE_INTERVAL_SEC = int(os.getenv('MAINTENANCE_INTERVAL_SEC', '21600'))
DB_VACUUM = os.getenv('DB_VACUUM', 'false').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
OLX_RETENTION_DAYS = int(os.getenv('OLX_RETENTION_DAYS', '0'))
SHAFA_RETENTION_DAYS = int(os.getenv('SHAFA_RETENTION_DAYS', '0'))

# Lightweight per-run header rotation (kept consistent during a single process run)
HEADER_PROFILES = [
    {
        "name": "chrome_win_124_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "accept_language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    },
    {
        "name": "chrome_win_123_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "accept_language": "uk,ru;q=0.9,en;q=0.8",
    },
    {
        "name": "firefox_win_122_uk",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "accept_language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    },
]
_header_profile = random.choice(HEADER_PROFILES)
RUN_USER_AGENT = _header_profile["user_agent"]
RUN_ACCEPT_LANGUAGE = _header_profile["accept_language"]

BASE_URLS = [
    { 
        "url": "https://www.lyst.com/shop/mens-shoes/?designer_slug=eytys&designer_slug=viron&designer_slug=vinnys&designer_slug=sapio&designer_slug=ranra&designer_slug=random-identities&designer_slug=ninamounah&designer_slug=namesake&designer_slug=hereu-designer&designer_slug=grenson&designer_slug=drae&designer_slug=copenhagen&designer_slug=a-diciannoveventitre&designer_slug=kleman&designer_slug=tiger-of-sweden&designer_slug=nanamica&designer_slug=marsell&designer_slug=norse-projects-arktisk&designer_slug=drae&designer_slug=maison-martin-margiela&designer_slug=magliano&designer_slug=mm6-by-maison-martin-margiela&designer_slug=camperlab&designer_slug=yume_yume&designer_slug=dries-van-noten&designer_slug=our-legacy&designer_slug=stefan-cooke&designer_slug=adieu&designer_slug=c2h4&designer_slug=lanvin&designer_slug=acne&designer_slug=a_cold_wall&designer_slug=raf-simons&designer_slug=raf-simons-runner&designer_slug=axel-arigato&designer_slug=y-3&designer_slug=prada&designer_slug=roa-designer&designer_slug=wooyoungmi&designer_slug=maison-margiela-x-reebok&designer_slug=424&designer_slug=jil-sander&designer_slug=juunj&designer_slug=kenzo&designer_slug=oamc&designer_slug=1017-alyx-9sm&designer_slug=officine-creative&designer_slug=marine-serre&designer_slug=salomon&designer_slug=moncler&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=ziggy-chen&designer_slug=comme-des-garcons&designer_slug=ami&designer_slug=rick-owens-drkshdw&designer_slug=marni&designer_slug=boris-bidjan-saberi-11&designer_slug=merrell&designer_slug=norse-projects&designer_slug=demon-designer&designer_slug=toga-virilis&designer_slug=gucci&designer_slug=kiko-kostadinov&designer_slug=fracap&designer_slug=golden-goose-deluxe-brand&designer_slug=sacai&designer_slug=ann-demeulemeester&designer_slug=diemme&designer_slug=gmbh&designer_slug=rombaut&designer_slug=both-paris&designer_slug=stone-island&designer_slug=jacquemus&designer_slug=jw-anderson&designer_slug=givenchy&designer_slug=sandro&designer_slug=hender-scheme&designer_slug=buttero&designer_slug=bottega-veneta&designer_slug=alexander-mcqueen&designer_slug=balenciaga&designer_slug=44-label-group&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=undercover&designer_slug=iro&designer_slug=ganni&designer_slug=ernest-w-baker&designer_slug=andersson-bell-designer&discount_from=60&final_price_from=0&final_price_to=250&instock_size=size.footwear.eu.u.40&instock_size=size.footwear.eu.u.40%275&instock_size=size.footwear.eu.u.41&sizes=IT+40.0&sizes=IT+40.5&sizes=IT+41.0&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
        "min_sale": 50,
        "url_name": "Grotesk Shoes"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=wooyoungmi&designer_slug=sulvam-designer&designer_slug=willy-chavarria-designer&designer_slug=tondolo&designer_slug=taiga-takahashi&designer_slug=tanaka-designer&designer_slug=stefan-cooke&designer_slug=stella-mccartney&designer_slug=situationist&designer_slug=small-talk-studio&designer_slug=secondlayer&designer_slug=saif-ud-deen&designer_slug=sankuanz&designer_slug=namacheko&designer_slug=adererror&designer_slug=egonlab&designer_slug=mugler&designer_slug=random-identities&designer_slug=m-i-s-b-h-v&designer_slug=eckhaus-latta&designer_slug=heliot-emil&designer_slug=undercover&designer_slug=sc103&designer_slug=ludovic-de-saint-sernin&designer_slug=ottolinger-designer&designer_slug=song-for-the-mute&designer_slug=uma-wang&designer_slug=dion-lee&designer_slug=jan-jan-van-essche&designer_slug=henrik-vibskov&designer_slug=feng-chen-wang-designer&designer_slug=givenchy&designer_slug=studio-nicholson&designer_slug=acne&designer_slug=ranra&designer_slug=rohe&designer_slug=amomento&designer_slug=meryll-rogge&designer_slug=carlota-barrera&designer_slug=han-kjobenhavn&designer_slug=affxwrks&designer_slug=alexander-wang&designer_slug=maisie-wilen&designer_slug=bianca-saunders&designer_slug=takahiromiyashita-thesoloist&designer_slug=saintwoods&designer_slug=berner-kuhl&designer_slug=arturo-obegero&designer_slug=spencer-badu&designer_slug=monitaly&designer_slug=44-label-group&designer_slug=filippa-k&designer_slug=saul-nash&designer_slug=serapis&designer_slug=frenckenberger&designer_slug=charles-jeffrey&designer_slug=we11done&designer_slug=mm6-by-maison-martin-margiela&designer_slug=maison-martin-margiela&designer_slug=marine-serre&designer_slug=luar-designer&designer_slug=soulland&designer_slug=dries-van-noten&designer_slug=chopova-lowena&designer_slug=fiorucci&designer_slug=bryan-jimenez&designer_slug=cormio&designer_slug=lownn&designer_slug=nemen-designer&designer_slug=lost-daze&designer_slug=rhude-designer&designer_slug=seekings&designer_slug=haulier&designer_slug=howlin-by-morrison&designer_slug=kanghyuk&designer_slug=kngsley&designer_slug=visvim&designer_slug=remi-relief&designer_slug=nanushka&discount_from=60&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.l&instock_size=size.jacket.eu.m.46&instock_size=size.jacket.eu.m.48&instock_size=size.jacket.eu.m.50&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48&instock_size=size.coat.eu.m.48&instock_size=size.coat.eu.m.52&sizes=M&sizes=L&sizes=S&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Main brands"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=anna-sui&designer_slug=fce&designer_slug=magliano&designer_slug=kijun&designer_slug=fax-copy-express&designer_slug=satoshi-nakamoto&designer_slug=darkpark&designer_slug=recto-designer&designer_slug=eytys&designer_slug=isaac-sellam&designer_slug=vetements&designer_slug=hed-mayner-designer&designer_slug=facetasm&designer_slug=denim-tears&designer_slug=daiwa-pier39&designer_slug=courreges&designer_slug=coperni&designer_slug=b1archive&designer_slug=alchemist&designer_slug=afb-designer&designer_slug=1989-studio&designer_slug=032c&designer_slug=424-on-fairfax&designer_slug=66-north&designer_slug=thug-club&designer_slug=south2-west8&designer_slug=sophnet&designer_slug=sasquatchfabrix&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=damir-doma&designer_slug=daniel-w-fletcher&designer_slug=martine-rose&designer_slug=dbyd&designer_slug=_jl-al_&designer_slug=deadwood-designer&designer_slug=pam-perks-and-mini&designer_slug=pdf&designer_slug=ann-demeulemeester&designer_slug=kapital&designer_slug=kidill&designer_slug=by-parrar&designer_slug=luu-an&designer_slug=luu-dan&designer_slug=goopimade&designer_slug=saint-mxxxxxx&designer_slug=saint-michael&designer_slug=jw-anderson&designer_slug=cole-buxton&designer_slug=raf-simons&designer_slug=m-i-s-b-h-v&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=ziggy-chen&designer_slug=entire-studios&designer_slug=kiko-kostadinov&designer_slug=tanaka-designer&designer_slug=a-better-mistake&designer_slug=rick-owens&designer_slug=pyer-moss&designer_slug=veilance&designer_slug=cav-empt&designer_slug=maharishi&designer_slug=lueder&designer_slug=casey-casey&designer_slug=document&designer_slug=ccp-designer&designer_slug=gimaguas&designer_slug=maryam-nassir-zadeh&designer_slug=schnaydermans&designer_slug=story-mfg&designer_slug=toogood&designer_slug=4sdesigns&designer_slug=roa-designer&designer_slug=stone-island-shadow-project&designer_slug=norse-projects-arktisk&designer_slug=toga&designer_slug=kuro&designer_slug=suicoke&designer_slug=abra&designer_slug=another-aspect&designer_slug=n-hoolywood&designer_slug=undercoverism-designer&designer_slug=hyein-seo&designer_slug=the-viridi-anne&designer_slug=jacquemus&designer_slug=soshiotsuki&designer_slug=jean-paul-gaultier&discount_from=60&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.l&instock_size=size.jacket.eu.m.46&instock_size=size.jacket.eu.m.48&instock_size=size.jacket.eu.m.50&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48&instock_size=size.coat.eu.m.48&instock_size=size.coat.eu.m.52&sizes=M&sizes=L&sizes=S&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Main brands [2]"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=cordera&designer_slug=hokita&designer_slug=vaquera&designer_slug=camiel-fortgens&designer_slug=tsau&designer_slug=y-3&designer_slug=carnet-archive&designer_slug=julius&designer_slug=vitelli-designer&designer_slug=peter-do&designer_slug=who-decides-war&designer_slug=lemaire-designerr&designer_slug=ganni&designer_slug=our-legacy&designer_slug=carne-bollente&designer_slug=kusikohc&designer_slug=issey-miyake-homme-plisse&designer_slug=objects-iv-life&designer_slug=andersson-bell-designer&designer_slug=c2h4&designer_slug=oamc&designer_slug=y-project&designer_slug=acronym&designer_slug=stone-island&designer_slug=boris-bidjan-saberi-11&designer_slug=ambush&designer_slug=and-wander&designer_slug=fce&designer_slug=byborre&designer_slug=doublet-designer&designer_slug=juunj&designer_slug=coperni&designer_slug=nicolas-andreas-taralis&designer_slug=post-archive-faction-paf&designer_slug=le17septembre&designer_slug=hodakova&designer_slug=hh-118389225&designer_slug=haal&designer_slug=gentle-fullness&designer_slug=eastwood-danso&designer_slug=dingyun-zhang&designer_slug=cotton-citizen&designer_slug=cornerstone&designer_slug=commission&designer_slug=comfy-outdoor-garment&designer_slug=cobra-sc&designer_slug=chenpeng&designer_slug=christian-dada&designer_slug=boramy-viguier&designer_slug=bless&designer_slug=birrot&designer_slug=arnar-mar-jonsson&designer_slug=archival-reinvent&designer_slug=bethany-williams&designer_slug=apc&designer_slug=andrej-gronau&designer_slug=alled-martinez&designer_slug=aie&designer_slug=adyar&designer_slug=abaga-velli&designer_slug=yoshio-kubo&designer_slug=xlim&designer_slug=willy-chavarria-designer&designer_slug=walter-van-beirendonck&designer_slug=styland&designer_slug=a-diciannoveventitre&designer_slug=hgbb-studio&designer_slug=cmmawear&designer_slug=uncertain-factor&designer_slug=maximilian-davis&designer_slug=the-product&designer_slug=mr-saturday&designer_slug=vein&designer_slug=meanswhile-designer&designer_slug=nrmous&designer_slug=sport-b-by-agnes-b&designer_slug=gmbh&designer_slug=satta-designer&designer_slug=sage-nation&designer_slug=rough&designer_slug=robyn-lynch&discount_from=60&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.l&instock_size=size.jacket.eu.m.46&instock_size=size.jacket.eu.m.48&instock_size=size.jacket.eu.m.50&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48&instock_size=size.coat.eu.m.48&instock_size=size.coat.eu.m.52&sizes=M&sizes=L&sizes=S&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Main brands [3]"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=louis-gabriel-nouchi&designer_slug=rough&designer_slug=rier&designer_slug=rainmaker-kyoto&designer_slug=paloma-wool&designer_slug=other-uk&designer_slug=omar-afridi&designer_slug=nuba&designer_slug=nullus&designer_slug=noma-td&designer_slug=nicholas-daley&designer_slug=needles&designer_slug=namesake&designer_slug=nanamica&designer_slug=martin-asbjorn&designer_slug=mastermind-world&designer_slug=maiden-name&designer_slug=lesugiatelier&designer_slug=international-gallery-beams&designer_slug=craig-green&designer_slug=strongthe&designer_slug=hope&designer_slug=yaku&designer_slug=holzweiler&designer_slug=simone-rocha&designer_slug=johnlawrencesullivan&designer_slug=gauchere&designer_slug=mainlinerusfrcadef&designer_slug=balenciaga&designer_slug=yohji-yamamoto&designer_slug=cout-de-la-liberte&designer_slug=rier&designer_slug=paura&designer_slug=paly&designer_slug=olly-shinder&designer_slug=nili-lotan&designer_slug=mlga&designer_slug=marina-yee&designer_slug=givenchy&designer_slug=kolor&designer_slug=juntae-kim&designer_slug=jordanluca&designer_slug=jiyongkim&designer_slug=_jl-al_&designer_slug=jl-al&designer_slug=isa-boulder&designer_slug=gimaguas&designer_slug=frei-mut&designer_slug=flaneur&designer_slug=croquis&designer_slug=cole-buxton&designer_slug=cmmn-swdn&designer_slug=charlie-constantinou&designer_slug=ceec&designer_slug=boris-bidjan-saberi&designer_slug=blst&designer_slug=blaest&designer_slug=av-vattev&designer_slug=another-aspect&designer_slug=airei&designer_slug=agolde&designer_slug=agnona&designer_slug=achilles-ion-gabriel&designer_slug=act-no1&designer_slug=low-classic&designer_slug=marni&designer_slug=aaspectrum&designer_slug=praying&designer_slug=rier&designer_slug=paly&designer_slug=vtmnts&designer_slug=mhl-by-margaret-howell&designer_slug=margaret-howell&designer_slug=meta-campania-collective&designer_slug=jil-sander&designer_slug=hysteric-glamour&designer_slug=applied-art-forms&designer_slug=aaron-esh&designer_slug=forma&designer_slug=ahluwalia&designer_slug=isabel-benenato&designer_slug=1017-alyx-9sm&designer_slug=toga-virilis&designer_slug=rrr123&designer_slug=random-identities&designer_slug=prototypes&designer_slug=pet-tree-kor&designer_slug=our-legacy&discount_from=60&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.l&instock_size=size.jacket.eu.m.46&instock_size=size.jacket.eu.m.48&instock_size=size.jacket.eu.m.50&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48&instock_size=size.coat.eu.m.48&instock_size=size.coat.eu.m.52&sizes=M&sizes=L&sizes=S&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Main brands [4]"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=ys-yohji-yamamoto&designer_slug=ouat&designer_slug=yleve&designer_slug=ys-for-men&designer_slug=the-product&designer_slug=ouat&designer_slug=vein&designer_slug=bed-jw-ford&designer_slug=meanswhile-designer&designer_slug=nrmous&designer_slug=kolor&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=fumito-ganryu&designer_slug=acronym&designer_slug=kozaburo&designer_slug=kaptain-sunshine&designer_slug=visvim&designer_slug=wtaps&designer_slug=99-is&designer_slug=devoa&designer_slug=attachment&designer_slug=sacai&designer_slug=issey-miyake&designer_slug=saul-nash&designer_slug=issey-miyake-homme-plisse&designer_slug=berner-kuhl&designer_slug=undercoverism-designer&designer_slug=yohji-yamamoto&designer_slug=peter-do&designer_slug=cordera&designer_slug=julius&designer_slug=hyein-seo&discount_from=60&final_price_to=200&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Japaneese brands"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?discount_from=68&final_price_from=50&final_price_to=180&retailer_slug=jomashop-us&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 50,
        "url_name": "Jomashop"
    },
    #    {
    #     "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=mastermind-japan&designer_slug=mastermind-world&discount_from=60",
    #     "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
    #     "min_sale": 50,
    #     "url_name": "test brands"
    # },
    # {
    #     "url": "https://www.lyst.com/shop/mens-shoes/?designer_slug=eytys&designer_slug=hereu-designer&designer_slug=grenson&designer_slug=drae&designer_slug=copenhagen&designer_slug=a-diciannoveventitre&designer_slug=kleman&designer_slug=tiger-of-sweden&designer_slug=nanamica&designer_slug=marsell&designer_slug=norse-projects-arktisk&designer_slug=drae&designer_slug=maison-martin-margiela&designer_slug=magliano&designer_slug=mm6-by-maison-martin-margiela&designer_slug=camperlab&designer_slug=yume_yume&designer_slug=dries-van-noten&designer_slug=our-legacy&designer_slug=stefan-cooke&designer_slug=adieu&designer_slug=c2h4&designer_slug=lanvin&designer_slug=acne&designer_slug=a_cold_wall&designer_slug=raf-simons&designer_slug=raf-simons-runner&designer_slug=axel-arigato&designer_slug=y-3&designer_slug=prada&designer_slug=roa-designer&designer_slug=wooyoungmi&designer_slug=maison-margiela-x-reebok&designer_slug=424&designer_slug=jil-sander&designer_slug=juunj&designer_slug=kenzo&designer_slug=oamc&designer_slug=1017-alyx-9sm&designer_slug=officine-creative&designer_slug=marine-serre&designer_slug=salomon&designer_slug=moncler&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=comme-des-garcons&designer_slug=ami&designer_slug=rick-owens-drkshdw&designer_slug=marni&designer_slug=boris-bidjan-saberi-11&designer_slug=merrell&designer_slug=norse-projects&designer_slug=demon-designer&designer_slug=toga-virilis&designer_slug=gucci&designer_slug=kiko-kostadinov&designer_slug=fracap&designer_slug=golden-goose-deluxe-brand&designer_slug=sacai&designer_slug=ann-demeulemeester&designer_slug=diemme&designer_slug=gmbh&designer_slug=rombaut&designer_slug=both-paris&designer_slug=stone-island&designer_slug=jacquemus&designer_slug=jw-anderson&designer_slug=givenchy&designer_slug=sandro&designer_slug=hender-scheme&designer_slug=buttero&designer_slug=bottega-veneta&designer_slug=alexander-mcqueen&designer_slug=balenciaga&designer_slug=44-label-group&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=undercover&designer_slug=iro&designer_slug=ganni&designer_slug=ernest-w-baker&designer_slug=andersson-bell-designer&discount_from=71&final_price_from=200&final_price_to=250&instock_size=size.footwear.eu.u.40&instock_size=size.footwear.eu.u.40%275&instock_size=size.footwear.eu.u.41&sizes=IT+40.0&sizes=IT+40.5&sizes=IT+41.0&view=price_asc",
    #     "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
    #     "min_sale": 50,
    #     "url_name": "Grotesk Pricy Shoes"
    # },
]

OLX_URLS = [
    { 
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/q-riri/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Riri"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-derek-rose/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Derek Rose"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-our-legacy/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Our Legacy"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Flat-Head/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Flat Head"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/q-Lady-White/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Lady White"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Wonder-Looper/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Wonder Looper"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-sunspel/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Sunspel"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Zimmerli/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=2000",
        "url_name": "Zimmerli"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Foscarini/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Foscarini"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-clinch/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Clinch"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/25lkx/",
        "url_name": "Руслана"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-James-Perse/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "James Perse"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/q-John-Smedley/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "John Smedley"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Agnona/?search%5Border%5D=created_at:desc",
        "url_name": "Agnona"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-ann-demeulemeester/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Ann Demeulemeester"
    },
    # {
    #     "url": "https://www.olx.ua/uk/list/q-visvim/?search%5Border%5D=created_at:desc",
    #     "url_name": "Visvim"
    # },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-carol-christian-poell/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Carol Christian Poell"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Hender-Scheme/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Hender Scheme"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-officine-creative/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Officine Creative"
    },
    {
        "url": "https://m.olx.ua/uk/moda-i-stil/q-schwanen/?search%5Border%5D=created_at:desc",
        "url_name": "Schwanen"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Iittala/?search%5Border%5D=created_at:desc",
        "url_name": "Iittala"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Georg-Jensen/?search%5Border%5D=created_at:desc",
        "url_name": "Georg Jensen"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Kartell/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Kartell"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Cassina/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Cassina"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/posuda-kuhonnaya-utvar/q-alessi/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Alessi"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Vitra/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Vitra"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Fritz-Hansen/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Fritz Hansen"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-BB-Italia/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "B&B Italia"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Knoll/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Knoll"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Carl-Hansen/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Carl Hansen & Søn"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Flexform/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Flexform"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Edra/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Edra"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Maxalto/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Maxalto"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Minotti/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Minotti"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Molteni/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Molteni"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-dornbracht/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=15000",
        "url_name": "Dornbracht"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/q-45rpm/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "45rpm"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Porro/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Porro "
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Zanotta/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Zanotta"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Rimadesio/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=50000",
        "url_name": "Rimadesio"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Moroso/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=50000",
        "url_name": "Moroso"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Tacchini/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=50000",
        "url_name": "Tacchini"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-antoniolupi/?search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=50000",
        "url_name": "antoniolupi"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/stroitelstvo-remont/santehnika/q-gessi/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=30000",
        "url_name": "Gessi"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/stroitelstvo-remont/santehnika/q-THG/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "THG Paris"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Davide-Groppi/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Davide Groppi"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Normann-Copenhagen/?search%5Border%5D=created_at:desc",
        "url_name": "Normann Copenhagen"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-L'Objet/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "L'Objet"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Audo-Copenhagen/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Audo Copenhagen"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Alias/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Alias"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Desalto/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Desalto"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Oluce/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Oluce"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Bonaldo/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Bonaldo"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/stroitelstvo-remont/santehnika/q-Graff/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Graff"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Martinelli-Luce/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Martinelli Luce"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/stroitelstvo-remont/santehnika/q-Axor/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=13000",
        "url_name": "Axor"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Saba-Italia/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Saba Italia"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Roche-Bobois/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=40000",
        "url_name": "Roche Bobois"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Ceccotti-Collezioni/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Ceccotti Collezioni"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Andreu-world/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Andreu World"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-La-Cividina/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "La Cividina"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Miniforms/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Miniforms"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Viccarbe/?search%5Border%5D=created_at:desc",
        "url_name": "Viccarbe"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-FontanaArte/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=12000",
        "url_name": "FontanaArte"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/mebel/q-Mattiazzi/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Mattiazzi"
    },  
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Punt-Mobles/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Punt Mobles"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-KEUCO/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "KEUCO"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-ClassiCon/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "ClassiCon"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Delta-light/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Delta Light"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-De-Castelli/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "De Castelli"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Ritzwell/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Ritzwell"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Brodware/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Brodware"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Eilersen/?search%5Border%5D=created_at:desc",
        "url_name": "Eilersen"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Leolux/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Leolux"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-liaigre/?search%5Border%5D=created_at:desc",
        "url_name": "Liaigre"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/83Rzs/",
        "url_name": "Гоша"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/4XOGU/",
        "url_name": "Андрій"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/2fnjBw/",
        "url_name": "Євгенія'"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Tonelli/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Tonelli"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Axolight/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Axolight"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Fima/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=20000",
        "url_name": "Fima"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-briggs-riley/?search%5Border%5D=created_at:desc",
        "url_name": "Briggs & Riley"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Skultuna/?search%5Border%5D=created_at:desc",
        "url_name": "Skultuna"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Azmaya/?search%5Border%5D=created_at:desc",
        "url_name": "Azmaya"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Linteloo/?search%5Border%5D=created_at:desc",
        "url_name": "Linteloo"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-paola-zani/?search%5Border%5D=created_at:desc",
        "url_name": "Paola Zani"
    },
     {
        "url": "https://www.olx.ua/uk/list/user/1OyGf/",
        "url_name": "Сергій Олігарх"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Frette/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Frette"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-arteluce/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Arteluce"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Verpan/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Verpan"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-NORR11/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "NORR11"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Victoria-Albert/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Victoria Albert"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Hender-Scheme/?search%5Border%5D=created_at:desc",
        "url_name": "Hender Scheme"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/verhnyaya-odezhda/puhoviki-zimnie-kurtki/q-moorer/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Moorer"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Artilect/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Artilect"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Nuyarn/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Nuyarn"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Alivar/?search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=40000",
        "url_name": "Alivar"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-USM-Haller/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "USM Haller"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-demeyere/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=6000",
        "url_name": "Demeyere"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Zwilling-twinox/?search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=2000",
        "url_name": "Zwilling Twinox"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Vitsoe/?search%5Border%5D=created_at:desc",
        "url_name": "Vitsoe"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-poltrona-frau/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=40000",
        "url_name": "Poltrona Frau"
    },
    {
        "url": "https://www.olx.ua/uk/elektronika/q-v-zug/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:from%5D=1000&search%5Bfilter_float_price:to%5D=30000",
        "url_name": "V-Zug"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Wilkhahn/?search%5Border%5D=created_at:desc",
        "url_name": "Wilkhahn"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Vollebak/?search%5Border%5D=created_at:desc",
        "url_name": "Vollebak"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/8LMcQ/",
        "url_name": "Андрій A"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Humanscale/?search%5Border%5D=created_at:desc",
        "url_name": "Humanscale"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/stroitelstvo-remont/q-boch-subway/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Boch Subway"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Hestan/?search%5Border%5D=created_at:desc",
        "url_name": "Hestan"
    },
    {
        "url": "https://www.olx.ua/uk/elektronika/q-Falmec/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:from%5D=1000&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Falmec"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Descente-Allterrain/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Descente Allterrain"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Fedeli/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Fedeli"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-helmut-lang/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Helmut Lang"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Veilance/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Veilance"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Filippo-De-Laurentiis/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=5000",
        "url_name": "Filippo De Laurentiis"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-drumohr/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Drumohr"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Avon-Celli/?search%5Border%5D=created_at:desc",
        "url_name": "Avon Celli"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Devoa/?search%5Border%5D=created_at%3Adesc",
        "url_name": "Devoa"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Stephan-Schneider/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Stephan Schneider"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Ziggy-Chen/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Ziggy Chen"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Barena-Venezia/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Barena Venezia"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Cornelian-Taurus/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Cornelian Taurus"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Margaret-Howell/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Margaret Howell"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Masnada/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Masnada"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Majestic-Filatures/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Majestic Filatures"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Outlier/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Outlier"
    },
    {
        "url": "https://www.olx.ua/uk/list/q-Fissler/?search%5Bfilter_enum_state%5D%5B0%5D=new&search%5Bfilter_float_price%3Ato%5D=6000&search%5Border%5D=created_at%3Adesc",
        "url_name": "Fissler"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Blurhms/?currency=UAH&search%5Border%5D=created_at%3Adesc",
        "url_name": "Blurhms"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/q-Graphpaper/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Graphpaper"
    }, 
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-Pillivuyt/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "Pillivuyt"
    },
    {
        "url": "https://www.olx.ua/uk/moda-i-stil/muzhskaya-odezhda/q-Herno-Laminar/?currency=UAH&search%5Border%5D=created_at:desc&search%5Bfilter_float_price:to%5D=10000",
        "url_name": "Herno Laminar"
    },
    {
        "url": "https://www.olx.ua/uk/dom-i-sad/q-all-clad/?currency=UAH&search%5Border%5D=created_at:desc",
        "url_name": "All Clad"
    },
    {
        "url": "https://www.olx.ua/uk/list/user/2hwtHL/#912656074",
        "url_name": "Олег"
    }
]

SHAFA_URLS = [
    {
        "url": "https://shafa.ua/uk/men?search_text=sunspel&sort=4",
        "url_name": "Sunspel"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=riri&sort=4",
        "url_name": "Riri"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Derek%20Rose&sort=4",
        "url_name": "Derek Rose"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Our%20Legacy&sort=4",
        "url_name": "Our Legacy"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Whitesville&sort=4",
        "url_name": "Whitesville"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Flat%20Head&sort=4",
        "url_name": "Flat Head"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Loopwheeler&sort=4",
        "url_name": "Loopwheeler"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Lady%20White&sort=4",
        "url_name": "Lady White"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Wonder%20Looper&sort=4",
        "url_name": "Wonder Looper"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Zimmerli&sort=4",
        "url_name": "Zimmerli"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=james%20perse&sort=4",
        "url_name": "James Perse"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=John%20Smedley&sort=4",
        "url_name": "John Smedley"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Agnona&sort=4",
        "url_name": "Agnona"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Ann%20Demeulemeester&sort=4",
        "url_name": "Ann Demeulemeester"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Carol%20Christian%20Poell&sort=4",
        "url_name": "Carol Christian Poell"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Hender%20Scheme&sort=4",
        "url_name": "Hender Scheme"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Officine%20Creative&sizes=174&sizes=239&sizes=176&sizes=240&sizes=175&sort=4",
        "url_name": "Officine Creative"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Schwanen&sort=4",
        "url_name": "Schwanen"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=45rpm&sort=4",
        "url_name": "45rpm"
    },
    {
        "url": "https://shafa.ua/uk/men?price_to=10000&search_text=dries%20van&sort=4",
        "url_name": "Dries Van Noten"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Moorer&sort=4",
        "url_name": "Moorer"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Artilect&sort=4",
        "url_name": "Artilect"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Nuyarn&sort=4",
        "url_name": "Nuyarn"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Vollebak&sort=4",
        "url_name": "Vollebak"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=visvim&sort=4",
        "url_name": "Visvim"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=allterain&sort=4",
        "url_name": "Allterrain"
    },
    {
        "url": "https://shafa.ua/clothes?search_text=Veilance&sort=4",
        "url_name": "Veilance"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=ten%20c&sort=4",
        "url_name": "Ten C"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Agnona&sort=4",
        "url_name": "Agnona"
    },
    {
        "url": "https://shafa.ua/uk/men?price_to=10000&search_text=Fedeli&sort=4",
        "url_name": "Fedeli"
    },
    {
        "url": "https://shafa.ua/uk/men?conditions=3&conditions=2&search_text=maison%20margiela&sort=4",
        "url_name": "Maison Margiela (M)"
    },
    # {
    #     "url": "https://shafa.ua/uk/women?conditions=3&conditions=2&search_text=maison%20margiela&sort=4",
    #     "url_name": "Maison Margiela (W)"
    # },
    {
        "url": "https://shafa.ua/uk/men?search_text=Helmut%20Lang&sort=4",
        "url_name": "Helmut Lang"
    },
    {
        "url": "https://shafa.ua/uk/clothes?conditions=3&conditions=2&search_text=ambush&sort=4",
        "url_name": "Ambush"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Herno%20Laminar&sort=4",
        "url_name": "Herno Laminar"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=tilak&sort=4",
        "url_name": "Tilak"
    },
    {
        "url": "https://shafa.ua/uk/men?price_to=10000&search_text=grenoble&sort=4",
        "url_name": "Moncler Grenoble"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Drumohr&sort=4",
        "url_name": "Drumohr"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Zanone&sort=4",
        "url_name": "Zanone"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Avon%20Celli&sort=4",
        "url_name": "Avon Celli"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=devoa&sort=4",
        "url_name": "Devoa"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Ziggy%20Chen&sort=4",
        "url_name": "Ziggy Chen"
    },
    {
        "url": "https://shafa.ua/uk/clothes?price_to=5000&search_text=Barena%20Venezia&sort=4",
        "url_name": "Barena Venezia"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Cornelian%20Taurus&sort=4",
        "url_name": "Cornelian Taurus"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Attachment%20&sort=4",
        "url_name": "Attachment"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Margaret%20Howell%20&sort=4",
        "url_name": "Margaret Howell"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Masnada%20&sort=4",
        "url_name": "Masnada"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Majestic%20Filatures%20&sort=4",
        "url_name": "Majestic Filatures"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Facetasm&sort=4",
        "url_name": "Facetasm"
    },
    {
        "url": "https://shafa.ua/uk/men?search_text=Jackman%20&sizes=148&sizes=151&sizes=150&sizes=149",
        "url_name": "Jackman"
    },
    # {
    #     "url": "https://shafa.ua/uk/men?price_to=3000&search_text=Malo%20&sort=4",
    #     "url_name": "Malo"
    # },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Outlier&sort=4",
        "url_name": "Outlier"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Blurhms&sort=4",
        "url_name": "Blurhms"
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Graphpaper&sort=4",
        "url_name": "Graphpaper"    
    },
    {
        "url": "https://shafa.ua/uk/clothes?search_text=Van%20Essche&sort=4",
        "url_name": "Van Essche"    
    }
]
