import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
EXCHANGERATE_API_KEY = os.getenv('EXCHANGERATE_API_KEY')
DANYLO_DEFAULT_CHAT_ID = os.getenv('DANYLO_DEFAULT_CHAT_ID')

BASE_URLS = [
    {
        "url": "https://www.lyst.com/shop/mens-shoes/?designer_slug=eytys&designer_slug=kleman&designer_slug=tiger-of-sweden&designer_slug=nanamica&designer_slug=marsell&designer_slug=norse-projects-arktisk&designer_slug=drae&designer_slug=maison-martin-margiela&designer_slug=magliano&designer_slug=mm6-by-maison-martin-margiela&designer_slug=camperlab&designer_slug=yume_yume&designer_slug=dries-van-noten&designer_slug=our-legacy&designer_slug=stefan-cooke&designer_slug=adieu&designer_slug=c2h4&designer_slug=lanvin&designer_slug=acne&designer_slug=a_cold_wall&designer_slug=raf-simons&designer_slug=raf-simons-runner&designer_slug=axel-arigato&designer_slug=y-3&designer_slug=prada&designer_slug=roa-designer&designer_slug=wooyoungmi&designer_slug=maison-margiela-x-reebok&designer_slug=424&designer_slug=jil-sander&designer_slug=juunj&designer_slug=kenzo&designer_slug=oamc&designer_slug=1017-alyx-9sm&designer_slug=officine-creative&designer_slug=marine-serre&designer_slug=salomon&designer_slug=moncler&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=comme-des-garcons&designer_slug=ami&designer_slug=rick-owens-drkshdw&designer_slug=marni&designer_slug=boris-bidjan-saberi-11&designer_slug=merrell&designer_slug=norse-projects&designer_slug=demon-designer&designer_slug=toga-virilis&designer_slug=gucci&designer_slug=kiko-kostadinov&designer_slug=fracap&designer_slug=golden-goose-deluxe-brand&designer_slug=sacai&designer_slug=ann-demeulemeester&designer_slug=diemme&designer_slug=gmbh&designer_slug=rombaut&designer_slug=both-paris&designer_slug=stone-island&designer_slug=jacquemus&designer_slug=jw-anderson&designer_slug=givenchy&designer_slug=sandro&designer_slug=hender-scheme&designer_slug=buttero&designer_slug=bottega-veneta&designer_slug=alexander-mcqueen&designer_slug=balenciaga&designer_slug=44-label-group&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=undercover&designer_slug=iro&designer_slug=ganni&designer_slug=ernest-w-baker&designer_slug=andersson-bell-designer&discount_from=62&final_price_from=0&final_price_to=200&instock_size=size.footwear.eu.u.40&instock_size=size.footwear.eu.u.40%275&instock_size=size.footwear.eu.u.41&sizes=IT+40.0&sizes=IT+40.5&sizes=IT+41.0&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
        "min_sale": 62,
        "url_name": "Grotesk Shoes"
    },
    {
        "url": "https://www.lyst.com/shop/mens-shoes/?designer_slug=eytys&designer_slug=kleman&designer_slug=tiger-of-sweden&designer_slug=nanamica&designer_slug=marsell&designer_slug=norse-projects-arktisk&designer_slug=drae&designer_slug=maison-martin-margiela&designer_slug=magliano&designer_slug=mm6-by-maison-martin-margiela&designer_slug=camperlab&designer_slug=yume_yume&designer_slug=dries-van-noten&designer_slug=our-legacy&designer_slug=stefan-cooke&designer_slug=adieu&designer_slug=c2h4&designer_slug=lanvin&designer_slug=acne&designer_slug=a_cold_wall&designer_slug=raf-simons&designer_slug=raf-simons-runner&designer_slug=axel-arigato&designer_slug=y-3&designer_slug=prada&designer_slug=roa-designer&designer_slug=wooyoungmi&designer_slug=maison-margiela-x-reebok&designer_slug=424&designer_slug=jil-sander&designer_slug=juunj&designer_slug=kenzo&designer_slug=oamc&designer_slug=1017-alyx-9sm&designer_slug=officine-creative&designer_slug=marine-serre&designer_slug=salomon&designer_slug=moncler&designer_slug=maison-mihara-yasuhiro-designer&designer_slug=comme-des-garcons&designer_slug=ami&designer_slug=rick-owens-drkshdw&designer_slug=marni&designer_slug=boris-bidjan-saberi-11&designer_slug=merrell&designer_slug=norse-projects&designer_slug=demon-designer&designer_slug=toga-virilis&designer_slug=gucci&designer_slug=kiko-kostadinov&designer_slug=fracap&designer_slug=golden-goose-deluxe-brand&designer_slug=sacai&designer_slug=ann-demeulemeester&designer_slug=diemme&designer_slug=gmbh&designer_slug=rombaut&designer_slug=both-paris&designer_slug=stone-island&designer_slug=jacquemus&designer_slug=jw-anderson&designer_slug=givenchy&designer_slug=sandro&designer_slug=hender-scheme&designer_slug=buttero&designer_slug=bottega-veneta&designer_slug=alexander-mcqueen&designer_slug=balenciaga&designer_slug=44-label-group&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=undercover&designer_slug=iro&designer_slug=ganni&designer_slug=ernest-w-baker&designer_slug=andersson-bell-designer&discount_from=71&final_price_from=200&final_price_to=250&instock_size=size.footwear.eu.u.40&instock_size=size.footwear.eu.u.40%275&instock_size=size.footwear.eu.u.41&sizes=IT+40.0&sizes=IT+40.5&sizes=IT+41.0&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
        "min_sale": 71,
        "url_name": "Grotesk Pricy Shoes"
    },
    {
        "url": "https://www.lyst.com/shop/mens-t-shirts/?designer_slug=a_cold_wall&designer_slug=givenchy&designer_slug=isabel-marant&designer_slug=burberry&designer_slug=charles-jeffrey&designer_slug=helmut-lang&designer_slug=neighborhood&designer_slug=aaspectrum&designer_slug=032c&designer_slug=acne&designer_slug=ambush&designer_slug=ami&designer_slug=andersson-bell-designer&designer_slug=boris-bidjan-saberi-11&designer_slug=boris-bidjan-saberi&designer_slug=c2h4&designer_slug=comme-des-garcons&designer_slug=craig-green&designer_slug=dries-van-noten&designer_slug=fce&designer_slug=eytys&designer_slug=feng-chen-wang-designer&designer_slug=gr10k&designer_slug=jacquemus&designer_slug=maison-martin-margiela&designer_slug=mm6-by-maison-martin-margiela&designer_slug=marine-serre&designer_slug=rick-owens&designer_slug=undercover&designer_slug=vetements&designer_slug=wales-bonner&designer_slug=y-3&designer_slug=ys-yohji-yamamoto&designer_slug=song-for-the-mute&designer_slug=acronym&designer_slug=objects-iv-life&designer_slug=post-archive-faction-paf&designer_slug=kusikohc&discount_from=60&final_price_to=100&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.l&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
        "min_sale": 60,
        "url_name": "Fucking T-shirts"
    },
    {
        "url": "https://www.lyst.com/shop/mens-pants/?designer_slug=aaspectrum&designer_slug=objects-iv-life&designer_slug=andersson-bell-designer&designer_slug=han-kjobenhavn&designer_slug=affxwrks&designer_slug=kozaburo&designer_slug=44-label-group&designer_slug=filippa-k&designer_slug=saul-nash&designer_slug=white-mountaineering&designer_slug=our-legacy&designer_slug=eytys&designer_slug=032c&designer_slug=dion-lee&designer_slug=jacquemus&designer_slug=ambush&designer_slug=marni&designer_slug=doublet-designer&designer_slug=low-classic&designer_slug=filippa-k&designer_slug=fce&designer_slug=feng-chen-wang-designer&designer_slug=saul-nash&designer_slug=heron-preston&designer_slug=anna-sui&designer_slug=we11done&designer_slug=c-p-company&designer_slug=recto-designer&designer_slug=rta&designer_slug=darkpark&designer_slug=spencer-badu&designer_slug=off-white-co-virgil-abloh&designer_slug=vetements&designer_slug=diesel&designer_slug=ami&designer_slug=acne&designer_slug=adererror&designer_slug=etudes-studio&designer_slug=vtmnts&designer_slug=1017-alyx-9sm&designer_slug=a_cold_wall&designer_slug=apc&designer_slug=kusikohc&discount_from=66&final_price_from=60&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
        "min_sale": 66,
        "url_name": "Fucking Pants"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=post-archive-faction-paf&designer_slug=raf-simons&designer_slug=m-i-s-b-h-v&designer_slug=gr10k&designer_slug=uma-wang&designer_slug=ziggy-chen&designer_slug=entire-studios&designer_slug=tanaka-designer&designer_slug=a-better-mistake&designer_slug=rick-owens&designer_slug=pyer-moss&designer_slug=veilance&designer_slug=cav-empt&designer_slug=maharishi&designer_slug=lueder&designer_slug=casey-casey&designer_slug=document&designer_slug=gimaguas&designer_slug=maryam-nassir-zadeh&designer_slug=schnaydermans&designer_slug=story-mfg&designer_slug=toogood&designer_slug=4sdesigns&designer_slug=roa-designer&designer_slug=stone-island-shadow-project&designer_slug=norse-projects-arktisk&designer_slug=kuro&designer_slug=suicoke&designer_slug=abra&designer_slug=another-aspect&designer_slug=n-hoolywood&designer_slug=undercoverism-designer&designer_slug=hyein-seo&designer_slug=the-viridi-anne&designer_slug=jacquemus&designer_slug=soshiotsuki&designer_slug=jean-paul-gaultier&designer_slug=louis-gabriel-nouchi&designer_slug=simone-rocha&designer_slug=johnlawrencesullivan&designer_slug=gauchere&designer_slug=mainlinerusfrcadef&designer_slug=yohji-yamamoto&designer_slug=cout-de-la-liberte&designer_slug=cordera&designer_slug=camiel-fortgens&designer_slug=julius&designer_slug=vitelli-designer&designer_slug=peter-do&designer_slug=who-decides-war&designer_slug=lemaire-designerr&designer_slug=ganni&designer_slug=our-legacy&designer_slug=carne-bollente&designer_slug=kusikohc&designer_slug=issey-miyake-homme-plisse&designer_slug=objects-iv-life&designer_slug=andersson-bell-designer&designer_slug=c2h4&designer_slug=oamc&designer_slug=y-project&designer_slug=acronym&designer_slug=stone-island&designer_slug=boris-bidjan-saberi-11&designer_slug=ambush&designer_slug=and-wander&designer_slug=fce&designer_slug=byborre&designer_slug=c-p-company&designer_slug=doublet-designer&designer_slug=juunj&designer_slug=coperni&designer_slug=nicolas-andreas-taralis&designer_slug=wooyoungmi&designer_slug=namacheko&designer_slug=adererror&designer_slug=egonlab&designer_slug=mugler&designer_slug=random-identities&designer_slug=m-i-s-b-h-v&designer_slug=eckhaus-latta&designer_slug=heliot-emil&designer_slug=undercover&designer_slug=sc103&designer_slug=ludovic-de-saint-sernin&designer_slug=ottolinger-designer&designer_slug=song-for-the-mute&designer_slug=uma-wang&designer_slug=dion-lee&designer_slug=jan-jan-van-essche&designer_slug=henrik-vibskov&designer_slug=feng-chen-wang-designer&designer_slug=givenchy&designer_slug=studio-nicholson&designer_slug=acne&designer_slug=ranra&designer_slug=rohe&designer_slug=amomento&designer_slug=meryll-rogge&designer_slug=carlota-barrera&designer_slug=han-kjobenhavn&designer_slug=affxwrks&designer_slug=alexander-wang&designer_slug=maisie-wilen&designer_slug=bianca-saunders&designer_slug=takahiromiyashita-thesoloist&designer_slug=saintwoods&designer_slug=berner-kuhl&designer_slug=arturo-obegero&designer_slug=spencer-badu&designer_slug=44-label-group&designer_slug=filippa-k&designer_slug=saul-nash&designer_slug=serapis&designer_slug=charles-jeffrey&designer_slug=we11done&designer_slug=mm6-by-maison-martin-margiela&designer_slug=maison-martin-margiela&designer_slug=marine-serre&designer_slug=soulland&designer_slug=dries-van-noten&designer_slug=chopova-lowena&designer_slug=fiorucci&designer_slug=bryan-jimenez&designer_slug=cormio&designer_slug=lownn&designer_slug=nemen-designer&designer_slug=lost-daze&designer_slug=rhude-designer&designer_slug=seekings&designer_slug=haulier&designer_slug=howlin-by-morrison&designer_slug=kanghyuk&designer_slug=kngsley&designer_slug=nanushka&designer_slug=visvim&designer_slug=remi-relief&designer_slug=nanushka&discount_from=64&final_price_to=200&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.t-shirt.international.m&instock_size=size.t-shirt.international.s&instock_size=size.t-shirt.international.l&instock_size=size.jacket.eu.m.46&instock_size=size.jacket.eu.m.48&instock_size=size.jacket.eu.m.50&instock_size=size.pants.eu.m.48&instock_size=size.pants.eu.m.46&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48&instock_size=size.coat.eu.m.48&instock_size=size.coat.eu.m.52&sizes=M&sizes=L&sizes=S&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 64,
        "url_name": "Cool brands"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?designer_slug=ys-yohji-yamamoto&designer_slug=fumito-ganryu&designer_slug=acronym&designer_slug=kozaburo&designer_slug=kaptain-sunshine&designer_slug=visvim&designer_slug=wtaps&designer_slug=99-is&designer_slug=devoa&designer_slug=attachment&designer_slug=sacai&designer_slug=issey-miyake&designer_slug=saul-nash&designer_slug=issey-miyake-homme-plisse&designer_slug=berner-kuhl&designer_slug=undercoverism-designer&designer_slug=yohji-yamamoto&designer_slug=peter-do&designer_slug=cordera&designer_slug=julius&designer_slug=hyein-seo&discount_from=60&final_price_to=200&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 60,
        "url_name": "Japaneese brands"
    },
    {
        "url": "https://www.lyst.com/shop/mens-clothing/?discount_from=68&final_price_from=50&final_price_to=180&retailer_slug=jomashop-us&view=price_asc",
        "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID, 
        "min_sale": 68,
        "url_name": "Jomashop"
    },
    #     {
    #     "url": "https://www.lyst.com/shop/mens-straight-jeans/?designer_slug=aaspectrum&designer_slug=white-mountaineering&designer_slug=our-legacy&designer_slug=eytys&designer_slug=032c&designer_slug=dion-lee&designer_slug=jacquemus&designer_slug=ambush&designer_slug=marni&designer_slug=doublet-designer&designer_slug=low-classic&designer_slug=filippa-k&designer_slug=fce&designer_slug=feng-chen-wang-designer&designer_slug=saul-nash&designer_slug=heron-preston&designer_slug=anna-sui&designer_slug=we11done&designer_slug=c-p-company&designer_slug=recto-designer&designer_slug=rta&designer_slug=darkpark&designer_slug=spencer-badu&designer_slug=off-white-co-virgil-abloh&designer_slug=vetements&designer_slug=diesel&designer_slug=ami&designer_slug=acne&designer_slug=adererror&designer_slug=etudes-studio&designer_slug=vtmnts&designer_slug=1017-alyx-9sm&designer_slug=a_cold_wall&designer_slug=apc&discount_from=70&final_price_from=70&final_price_to=200&view=price_asc",
    #     "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
    #     "min_sale": 40,
    #     "url_name": "Fucking jeans"
    # },
    #       {
    #     "url": "https://www.lyst.com/shop/mens-bermuda-shorts/?designer_slug=dries-van-noten&designer_slug=ami&designer_slug=apc&designer_slug=a_cold_wall&designer_slug=carhartt&designer_slug=diesel&designer_slug=etudes-studio&designer_slug=gcds&designer_slug=haikure&designer_slug=helmut-lang&designer_slug=ih-nom-uh-nit&designer_slug=kenzo&designer_slug=msgm&designer_slug=norse-projects&designer_slug=represent&designer_slug=sandro&designer_slug=sporty-rich&designer_slug=truenyc&designer_slug=y-3&designer_slug=424&designer_slug=1017-alyx-9sm&discount_from=50&final_price_to=100&instock_size=size.shorts.eu.m.46&instock_size=size.shorts.eu.m.48",
    #     "telegram_chat_id": DANYLO_DEFAULT_CHAT_ID,
    #     "min_sale": 65,
    #     "url_name": "Fucking shorts"
    # },
]