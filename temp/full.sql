﻿CREATE TABLE "CLT_PM_W_ERIC_VCLTP" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMRECEIVEDATMCELLS" NUMBER(13,2), 
	"PMTRANSMITTEDATMCELLS" NUMBER(13,2), 
	"TRANSPORTNETWORK" VARCHAR2(300), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"ATMPORT" VARCHAR2(300), 
	"VPLTP" VARCHAR2(300), 
	"VPCTP" VARCHAR2(300), 
	"VCLTP" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   ) ;
   
CREATE TABLE  "CLT_PM_W_ERIC_UTRANRELATION" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"COL_340" NUMBER(13,2), 
	"COL_341" NUMBER(13,2), 
	"COL_342" NUMBER(13,2), 
	"PMRLADDATTEMPTSBESTCELLSPEECH" NUMBER(13,2), 
	"COL_343" NUMBER(13,2), 
	"PMRLADDATTEMPTSBESTCELLSTREAM" NUMBER(13,2), 
	"COL_344" NUMBER(13,2), 
	"COL_345" NUMBER(13,2), 
	"COL_346" NUMBER(13,2), 
	"PMRLADDSUCCESSBESTCELLSPEECH" NUMBER(13,2), 
	"COL_347" NUMBER(13,2), 
	"PMRLADDSUCCESSBESTCELLSTREAM" NUMBER(13,2), 
	"PMNOATTOUTCNHHOCSNONSPEECH" NUMBER(13,2), 
	"PMNOATTOUTCNHHOSPEECH" NUMBER(13,2), 
	"PMNOSUCCOUTCNHHOCSNONSPEECH" NUMBER(13,2), 
	"PMNOSUCCOUTCNHHOSPEECH" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"UTRANCELL" VARCHAR2(300), 
	"UTRANRELATION" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   );
   
   CREATE TABLE "CLT_PM_W_ERIC_UTRANCELL" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMNOTIMESRLDELFRACTSET" NUMBER(13,2), 
	"PMNOTIMESRLADDTOACTSET" NUMBER(13,2), 
	"PMNOTIMESRLREPINACTSET" NUMBER(13,2), 
	"PMNOTIMESCELLFAILADDTOACTSET" NUMBER(13,2), 
	"PMNOCELLUPDATTEMPT" NUMBER(13,2), 
	"PMNOCELLUPDSUCCESS" NUMBER(13,2), 
	"PMNOCELLDCHDISCONNECTNORMAL" NUMBER(13,2), 
	"PMNOCELLFACHDISCONNECTNORMAL" NUMBER(13,2), 
	"PMNOCELLDCHDISCONNECTABNORM" NUMBER(13,2), 
	"PMNOCELLFACHDISCONNECTABNORM" NUMBER(13,2), 
	"PMNOSPEECHDCHDISCNORMAL" NUMBER(13,2), 
	"PMNOSPEECHDCHDISCABNORM" NUMBER(13,2), 
	"PMNOPACKETDCHDISCNORMAL" NUMBER(13,2), 
	"PMNOPACKETDCHDISCABNORM" NUMBER(13,2), 
	"PMNOCS64DCHDISCNORMAL" NUMBER(13,2), 
	"PMNOCS64DCHDISCABNORM" NUMBER(13,2), 
	"PMTOTNOUTRANREJRRCCONNREQ" NUMBER(13,2), 
	"PMNOREJRRCCONNMPLOADC" NUMBER(13,2), 
	"PMNOOFSAMPASEUL" NUMBER(13,2), 
	"PMSUMOFSAMPASEUL" NUMBER(13,2), 
	"PMNOOFSAMPASEDL" NUMBER(13,2), 
	"PMSUMOFSAMPASEDL" NUMBER(13,2), 
	"PMSUMOFTIMESMEASOLDL" NUMBER(13,2), 
	"PMSUMOFTIMESMEASOLUL" NUMBER(13,2), 
	"PMNOREQDENIEDADM" NUMBER(13,2), 
	"PMNOFAILEDAFTERADM" NUMBER(13,2), 
	"PMNOOFSWDOWNNGADM" NUMBER(13,2), 
	"PMNOOFRLFORDRIFTINGUES" NUMBER(13,2), 
	"PMNOOFRLFORNONDRIFTINGUES" NUMBER(13,2), 
	"PMNOOFRETURNINGEMERGENCYCALLS" NUMBER(13,2), 
	"PMNOOFRETURNINGRRCCONN" NUMBER(13,2), 
	"PMNODIRRETRYSUCCESS" NUMBER(13,2), 
	"PMNOSYSRELSPEECHSOHO" NUMBER(13,2), 
	"PMNOSYSRELSPEECHNEIGHBR" NUMBER(13,2), 
	"PMNOSYSRELSPEECHULSYNCH" NUMBER(13,2), 
	"COL_235" NUMBER(13,2), 
	"COL_236" NUMBER(13,2), 
	"COL_237" NUMBER(13,2), 
	"COL_238" NUMBER(13,2), 
	"COL_239" NUMBER(13,2), 
	"PMSAMPLESCS64PS8RABESTABLISH" NUMBER(13,2), 
	"PMSUMCS64PS8RABESTABLISH" NUMBER(13,2), 
	"PMNOINCOMINGHSHARDHOSUCCESS" NUMBER(13,2), 
	"PMNOOUTGOINGHSHARDHOSUCCESS" NUMBER(13,2), 
	"PMNOHSCCATTEMPT" NUMBER(13,2), 
	"PMNOHSCCSUCCESS" NUMBER(13,2), 
	"PMSAMPLESPSHSADCHRABESTABLISH" NUMBER(13,2), 
	"PMSUMPSHSADCHRABESTABLISH" NUMBER(13,2), 
	"COL_240" NUMBER(13,2), 
	"PMSUMPSSTR128PS8RABESTABLISH" NUMBER(13,2), 
	"PMINACTIVITYPSSTREAMIDLE" NUMBER(13,2), 
	"COL_241" NUMBER(13,2), 
	"PMSUMBESTCS12PSINTRABESTABLISH" NUMBER(13,2), 
	"COL_242" NUMBER(13,2), 
	"PMSUMBESTCS64PSINTRABESTABLISH" NUMBER(13,2), 
	"PMDOWNSWITCHATTEMPT" NUMBER(13,2), 
	"PMDOWNSWITCHSUCCESS" NUMBER(13,2), 
	"PMULUPSWITCHSUCCESSMEDIUM" NUMBER(13,2), 
	"PMULUPSWITCHATTEMPTMEDIUM" NUMBER(13,2), 
	"PMUPSWITCHFACHHSSUCCESS" NUMBER(13,2), 
	"PMUPSWITCHFACHHSATTEMPT" NUMBER(13,2), 
	"PMULUPSWITCHSUCCESSLOW" NUMBER(13,2), 
	"PMULUPSWITCHATTEMPTLOW" NUMBER(13,2), 
	"PMULUPSWITCHSUCCESSHIGH" NUMBER(13,2), 
	"PMULUPSWITCHATTEMPTHIGH" NUMBER(13,2), 
	"PMDLUPSWITCHSUCCESSHS" NUMBER(13,2), 
	"PMDLUPSWITCHATTEMPTHS" NUMBER(13,2), 
	"PMCHSWITCHATTEMPTFACHURA" NUMBER(13,2), 
	"PMCHSWITCHSUCCFACHURA" NUMBER(13,2), 
	"PMCHSWITCHATTEMPTURAFACH" NUMBER(13,2), 
	"PMCHSWITCHSUCCURAFACH" NUMBER(13,2), 
	"PMNONORMALRABRELEASEPACKETURA" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEPACKETURA" NUMBER(13,2), 
	"PMNOURAUPDATTEMPT" NUMBER(13,2), 
	"PMNOURAUPDSUCCESS" NUMBER(13,2), 
	"PMEULHARQTRANSMTTI10FAILURE" NUMBER(13,2), 
	"PMULUPSWITCHATTEMPTEUL" NUMBER(13,2), 
	"PMULUPSWITCHSUCCESSEUL" NUMBER(13,2), 
	"PMSAMPLESPSEULRABESTABLISH" NUMBER(13,2), 
	"PMSUMPSEULRABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESULRSSI" NUMBER(13,2), 
	"PMSUMULRSSI" NUMBER(13,2), 
	"PMDLUPSWITCHATTEMPTLOW" NUMBER(13,2), 
	"PMDLUPSWITCHATTEMPTMEDIUM" NUMBER(13,2), 
	"PMDLUPSWITCHATTEMPTHIGH" NUMBER(13,2), 
	"PMDLUPSWITCHSUCCESSLOW" NUMBER(13,2), 
	"PMDLUPSWITCHSUCCESSMEDIUM" NUMBER(13,2), 
	"PMDLUPSWITCHSUCCESSHIGH" NUMBER(13,2), 
	"COL_243" NUMBER(13,2), 
	"COL_244" NUMBER(13,2), 
	"COL_245" NUMBER(13,2), 
	"COL_246" NUMBER(13,2), 
	"COL_247" NUMBER(13,2), 
	"COL_248" NUMBER(13,2), 
	"COL_249" NUMBER(13,2), 
	"PMNODISCARDSDUDTCHHSPSSTREAM" NUMBER(13,2), 
	"PMNORECEIVEDSDUDTCHHSPSSTREAM" NUMBER(13,2), 
	"PMNOPSSTREAMHSCCATTEMPT" NUMBER(13,2), 
	"PMNOPSSTREAMHSCCSUCCESS" NUMBER(13,2), 
	"COL_250" NUMBER(13,2), 
	"PMNORABESTBLOCKTNSPEECHBEST" NUMBER(13,2), 
	"PMNORABESTBLOCKTNCS57BEST" NUMBER(13,2), 
	"PMNORABESTBLOCKTNCS64BEST" NUMBER(13,2), 
	"COL_251" NUMBER(13,2), 
	"COL_252" NUMBER(13,2), 
	"PMNORABESTBLOCKTNPSINTHSBEST" NUMBER(13,2), 
	"PMNORABESTBLOCKTNPSSTRHS" NUMBER(13,2), 
	"PMNORABESTBLOCKTNSPEECH" NUMBER(13,2), 
	"PMNORABESTBLOCKTNCS57" NUMBER(13,2), 
	"PMNORABESTBLOCKTNCS64" NUMBER(13,2), 
	"PMNORABESTBLOCKTNPSSTRNONHS" NUMBER(13,2), 
	"PMNORABESTBLOCKTNPSINTNONHS" NUMBER(13,2), 
	"PMNORABESTBLOCKTNPSINTHS" NUMBER(13,2), 
	"PMNORRCCONNREQBLOCKTNCS" NUMBER(13,2), 
	"PMNORRCCONNREQBLOCKTNPS" NUMBER(13,2), 
	"PMNORABESTBLOCKNODESPEECHBEST" NUMBER(13,2), 
	"PMNORABESTBLOCKNODECS57BEST" NUMBER(13,2), 
	"PMNORABESTBLOCKNODECS64BEST" NUMBER(13,2), 
	"COL_253" NUMBER(13,2), 
	"PMNORABESTBLOCKNODEPSINTHSBEST" NUMBER(13,2), 
	"PMNORABESTBLOCKNODEPSSTRHSBEST" NUMBER(13,2), 
	"COL_254" NUMBER(13,2), 
	"PMNORRCCONNREQBLOCKNODECS" NUMBER(13,2), 
	"PMNORRCCONNREQBLOCKNODEPS" NUMBER(13,2), 
	"PMSUMSQRULRSSI" NUMBER(13,2), 
	"PMSUMDLCODE" NUMBER(13,2), 
	"PMSUMSQRDLCODE" NUMBER(13,2), 
	"PMSAMPLESDLCODE" NUMBER(13,2), 
	"PMNOFAILEDRRCCONNECTREQCSHW" NUMBER(13,2), 
	"PMNOFAILEDRRCCONNECTREQPSHW" NUMBER(13,2), 
	"PMNOFAILEDRRCCONNECTREQHW" NUMBER(13,2), 
	"PMDCHULRLCUSERPACKETTHP" VARCHAR2(200), 
	"PMDCHDLRLCUSERPACKETTHP" VARCHAR2(200), 
	"COL_255" VARCHAR2(200), 
	"PMEULHARQTRANSMTTI10SRB" VARCHAR2(200), 
	"PMSUMPACKETDLDELAY" VARCHAR2(200), 
	"PMSAMPLESPACKETDLDELAY" VARCHAR2(200), 
	"PMEULHARQTRANSMTTI2PSRABS" VARCHAR2(200), 
	"PMEULHARQTRANSMTTI2SRB" VARCHAR2(200), 
	"PMNOCSSTREAMDCHDISCNORMAL" NUMBER(13,2), 
	"PMNOCSSTREAMDCHDISCABNORM" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQ" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQSUCCESS" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQCS" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQCSSUCC" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQPS" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQPSSUCC" NUMBER(13,2), 
	"PMCHSWITCHFACHIDLE" NUMBER(13,2), 
	"PMSAMPLESCS12RABESTABLISH" NUMBER(13,2), 
	"PMSUMCS12RABESTABLISH" NUMBER(13,2), 
	"PMNOLOADSHARINGRRCCONN" NUMBER(13,2), 
	"PMNOPSSTREAM64PS8DCHDISCNORMAL" NUMBER(13,2), 
	"PMNOPSSTREAM64PS8DCHDISCABNORM" NUMBER(13,2), 
	"PMSAMPLESBESTCS12ESTABLISH" NUMBER(13,2), 
	"PMSUMBESTCS12ESTABLISH" NUMBER(13,2), 
	"PMNODIRRETRYATT" NUMBER(13,2), 
	"PMCELLDOWNTIMEAUTO" NUMBER(13,2), 
	"PMCELLDOWNTIMEMAN" NUMBER(13,2), 
	"PMTOTALTIMEULCELLCONG" NUMBER(13,2), 
	"PMTOTALTIMEDLCELLCONG" NUMBER(13,2), 
	"PMNOOFNONHOREQDENIEDSPEECH" NUMBER(13,2), 
	"PMNOOFNONHOREQDENIEDCS" NUMBER(13,2), 
	"COL_256" NUMBER(13,2), 
	"PMNORABESTABLISHATTEMPTSPEECH" NUMBER(13,2), 
	"PMNORABESTABLISHATTEMPTCS64" NUMBER(13,2), 
	"PMNORABESTABLISHATTEMPTCS57" NUMBER(13,2), 
	"COL_257" NUMBER(13,2), 
	"COL_258" NUMBER(13,2), 
	"PMNORABESTABLISHSUCCESSSPEECH" NUMBER(13,2), 
	"PMNORABESTABLISHSUCCESSCS64" NUMBER(13,2), 
	"PMNORABESTABLISHSUCCESSCS57" NUMBER(13,2), 
	"COL_259" NUMBER(13,2), 
	"COL_260" NUMBER(13,2), 
	"PMNONORMALRABRELEASEPACKET" NUMBER(13,2), 
	"PMNONORMALRABRELEASESPEECH" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEPACKET" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASESPEECH" NUMBER(13,2), 
	"COL_261" NUMBER(13,2), 
	"COL_262" NUMBER(13,2), 
	"PMNONORMALRABRELEASECSSTREAM" NUMBER(13,2), 
	"PMNONORMALRABRELEASECS64" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASECSSTREAM" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASECS64" NUMBER(13,2), 
	"COL_263" NUMBER(13,2), 
	"COL_264" NUMBER(13,2), 
	"COL_265" NUMBER(13,2), 
	"PMNONORMALRBRELEASEHS" NUMBER(13,2), 
	"PMNOSYSTEMRBRELEASEHS" NUMBER(13,2), 
	"PMNOINCOMINGHSHARDHOATTEMPT" NUMBER(13,2), 
	"PMNOOUTGOINGHSHARDHOATTEMPT" NUMBER(13,2), 
	"PMNOHSHARDHORETURNOLDCHSOURCE" NUMBER(13,2), 
	"PMNOHSHARDHORETURNOLDCHTARGET" NUMBER(13,2), 
	"PMNOOFNONHOREQDENIEDHS" NUMBER(13,2), 
	"COL_266" NUMBER(13,2), 
	"PMSUMBESTPSHSADCHRABESTABLISH" NUMBER(13,2), 
	"COL_267" NUMBER(13,2), 
	"COL_268" NUMBER(13,2), 
	"COL_269" NUMBER(13,2), 
	"COL_270" NUMBER(13,2), 
	"COL_271" NUMBER(13,2), 
	"COL_272" NUMBER(13,2), 
	"PMNOOFNONHOREQDENIEDPSSTR128" NUMBER(13,2), 
	"PMSAMPLESBESTCS57RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTCS57RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESBESTCS64RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTCS64RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESFACHPSINTRABESTABLISH" NUMBER(13,2), 
	"PMSUMFACHPSINTRABESTABLISH" NUMBER(13,2), 
	"COL_273" NUMBER(13,2), 
	"PMSUMBESTDCHPSINTRABESTABLISH" NUMBER(13,2), 
	"COL_274" NUMBER(13,2), 
	"COL_275" NUMBER(13,2), 
	"PMSAMPLESDCHULRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSUMDCHULRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESDCHDLRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSUMDCHDLRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESDCHULRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSUMDCHULRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESDCHDLRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSUMDCHDLRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMNORRCCSREQDENIEDADM" NUMBER(13,2), 
	"PMSAMPLESPSINTERACTIVE" NUMBER(13,2), 
	"PMSUMPSINTERACTIVE" NUMBER(13,2), 
	"COL_276" NUMBER(13,2), 
	"COL_277" NUMBER(13,2), 
	"PMNOSYSTEMRBRELEASEEUL" NUMBER(13,2), 
	"PMNONORMALRBRELEASEEUL" NUMBER(13,2), 
	"PMSAMPLESBESTPSEULRABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTPSEULRABESTABLISH" NUMBER(13,2), 
	"PMRABESTABLISHECATTEMPT" NUMBER(13,2), 
	"PMRABESTABLISHECSUCCESS" NUMBER(13,2), 
	"PMNOOFNONHOREQDENIEDEUL" NUMBER(13,2), 
	"PMSUMULRLCUSERTHPPSSTREAM16" NUMBER(13,2), 
	"COL_278" NUMBER(13,2), 
	"PMSUMDLRLCUSERTHPPSSTREAM64" NUMBER(13,2), 
	"COL_279" NUMBER(13,2), 
	"PMSUMDLRLCUSERTHPPSSTREAM128" NUMBER(13,2), 
	"COL_280" NUMBER(13,2), 
	"PMNORABESTATTEMPTPSSTREAMHS" NUMBER(13,2), 
	"PMNORABESTSUCCESSPSSTREAMHS" NUMBER(13,2), 
	"PMNONORMALRABRELEASEPSSTREAMHS" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEPSSTREAMHS" NUMBER(13,2), 
	"PMSAMPLESPSSTREAMHSRABEST" NUMBER(13,2), 
	"PMSUMPSSTREAMHSRABEST" NUMBER(13,2), 
	"PMSAMPLESBESTPSSTREAMHSRABEST" NUMBER(13,2), 
	"PMSUMBESTPSSTREAMHSRABEST" NUMBER(13,2), 
	"PMSUMDLRLCUSERTHPPSSTREAMHS" NUMBER(13,2), 
	"COL_281" NUMBER(13,2), 
	"PMSUMULRLCUSERTHPPSSTREAM32" NUMBER(13,2), 
	"COL_282" NUMBER(13,2), 
	"COL_283" NUMBER(13,2), 
	"COL_284" NUMBER(13,2), 
	"COL_285" NUMBER(13,2), 
	"COL_286" NUMBER(13,2), 
	"COL_287" NUMBER(13,2), 
	"COL_288" NUMBER(13,2), 
	"PMSUMULRLCUSERTHPPSSTREAM128" NUMBER(13,2), 
	"COL_289" NUMBER(13,2), 
	"PMNOLOADSHARINGRRCCONNCS" NUMBER(13,2), 
	"PMNOLOADSHARINGRRCCONNPS" NUMBER(13,2), 
	"PMNORABESTATTEMPTPSINTNONHS" NUMBER(13,2), 
	"PMNORABESTSUCCESSPSINTNONHS" NUMBER(13,2), 
	"PMSUMPACKETLATENCY" VARCHAR2(200), 
	"PMSAMPLESPACKETLATENCY" VARCHAR2(200), 
	"PMSUMPACKETLATENCYPSSTREAMHS" VARCHAR2(200), 
	"COL_290" VARCHAR2(200), 
	"PMDLTRAFFICVOLUMEPSINTHS" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPSINTEUL" NUMBER(13,2), 
	"PMFAULTYTRANSPORTBLOCKSBCUL" NUMBER(13,2), 
	"PMTRANSPORTBLOCKSBCUL" NUMBER(13,2), 
	"PMNOOFTERMSPEECHCONG" NUMBER(13,2), 
	"PMENABLEEULHHOATTEMPT" NUMBER(13,2), 
	"PMENABLEEULHHOSUCCESS" NUMBER(13,2), 
	"PMENABLEHSHHOATTEMPT" NUMBER(13,2), 
	"PMENABLEHSHHOSUCCESS" NUMBER(13,2), 
	"PMNOINCOMINGEULHARDHOATTEMPT" NUMBER(13,2), 
	"PMNOINCOMINGEULHARDHOSUCCESS" NUMBER(13,2), 
	"PMNOOUTGOINGEULHARDHOATTEMPT" NUMBER(13,2), 
	"PMNOOUTGOINGEULHARDHOSUCCESS" NUMBER(13,2), 
	"PMNORRCPSREQDENIEDADM" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQPS" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQPSSUCC" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQSUCC" NUMBER(13,2), 
	"PMTOTALTIMEHSDSCHOVERLOAD" NUMBER(13,2), 
	"PMNOPAGINGTYPE1ATTEMPTCS" NUMBER(13,2), 
	"PMNOPAGINGTYPE1ATTEMPTPS" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQCS" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEAMR4750" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEAMR5900" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEAMR7950" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEAMRNBMM" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEAMRWB" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMECS12" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMECS57" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMECS64" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPS128" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPS16" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPS384" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPS64" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPS8" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPSCOMMON" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPSSTR128" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPSSTR16" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPSSTR64" NUMBER(13,2), 
	"PMDLTRAFFICVOLUMEPSSTRHS" NUMBER(13,2), 
	"PMEULTODCHATTEMPT" NUMBER(13,2), 
	"PMEULTODCHSUCCESS" NUMBER(13,2), 
	"PMHSTODCHATTEMPT" NUMBER(13,2), 
	"PMHSTODCHSUCCESS" NUMBER(13,2), 
	"PMNOEULCCATTEMPT" NUMBER(13,2), 
	"PMNOEULCCSUCCESS" NUMBER(13,2), 
	"PMNOINCSIRATHOADMFAIL" NUMBER(13,2), 
	"PMNOINCSIRATHOATT" NUMBER(13,2), 
	"PMNOINCSIRATHOSUCCESS" NUMBER(13,2), 
	"PMNONORMALRABRELEASEAMRNB" NUMBER(13,2), 
	"PMNONORMALRABRELEASEAMRWB" NUMBER(13,2), 
	"PMNOPAGINGATTEMPTCNINITDCCH" NUMBER(13,2), 
	"PMNOPAGINGTYPE1ATTEMPT" NUMBER(13,2), 
	"PMNORLDENIEDADM" NUMBER(13,2), 
	"PMNORRCREQDENIEDADM" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEAMRNB" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEAMRWB" NUMBER(13,2), 
	"COL_291" NUMBER(13,2), 
	"COL_292" NUMBER(13,2), 
	"COL_293" NUMBER(13,2), 
	"PMRLADDATTEMPTSBESTCELLSPEECH" NUMBER(13,2), 
	"COL_294" NUMBER(13,2), 
	"PMRLADDATTEMPTSBESTCELLSTREAM" NUMBER(13,2), 
	"COL_295" NUMBER(13,2), 
	"COL_296" NUMBER(13,2), 
	"COL_297" NUMBER(13,2), 
	"PMRLADDSUCCESSBESTCELLSPEECH" NUMBER(13,2), 
	"COL_298" NUMBER(13,2), 
	"PMRLADDSUCCESSBESTCELLSTREAM" NUMBER(13,2), 
	"PMSAMPLESAMR12200RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESAMR4750RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESAMR5900RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESAMR7950RABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESAMRNBMMRABESTABLISH" NUMBER(13,2), 
	"PMSAMPLESAMRWBRABESTABLISH" NUMBER(13,2), 
	"COL_299" NUMBER(13,2), 
	"COL_300" NUMBER(13,2), 
	"COL_301" NUMBER(13,2), 
	"COL_302" NUMBER(13,2), 
	"COL_303" NUMBER(13,2), 
	"PMSAMPLESBESTAMRWBRABESTABLISH" NUMBER(13,2), 
	"COL_304" NUMBER(13,2), 
	"COL_305" NUMBER(13,2), 
	"COL_306" NUMBER(13,2), 
	"COL_307" NUMBER(13,2), 
	"COL_308" NUMBER(13,2), 
	"COL_309" NUMBER(13,2), 
	"COL_310" NUMBER(13,2), 
	"COL_311" NUMBER(13,2), 
	"COL_312" NUMBER(13,2), 
	"PMSUMAMR12200RABESTABLISH" NUMBER(13,2), 
	"PMSUMAMR4750RABESTABLISH" NUMBER(13,2), 
	"PMSUMAMR5900RABESTABLISH" NUMBER(13,2), 
	"PMSUMAMR7950RABESTABLISH" NUMBER(13,2), 
	"PMSUMAMRNBMMRABESTABLISH" NUMBER(13,2), 
	"PMSUMAMRWBRABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMR12200RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMR4750RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMR5900RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMR7950RABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMRNBMMRABESTABLISH" NUMBER(13,2), 
	"PMSUMBESTAMRWBRABESTABLISH" NUMBER(13,2), 
	"PMSUMUESWITH1RLS1RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH1RLS2RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH1RLS3RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH2RLS2RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH2RLS3RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH2RLS4RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH3RLS3RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH3RLS4RLINACTSET" NUMBER(13,2), 
	"PMSUMUESWITH4RLS4RLINACTSET" NUMBER(13,2), 
	"COL_313" NUMBER(13,2), 
	"COL_314" NUMBER(13,2), 
	"COL_315" NUMBER(13,2), 
	"COL_316" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQ" NUMBER(13,2), 
	"PMTOTNOTERMRRCCONNECTREQCSSUCC" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEAMR4750" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEAMR5900" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEAMR7950" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEAMRNBMM" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEAMRWB" NUMBER(13,2), 
	"PMULTRAFFICVOLUMECS12" NUMBER(13,2), 
	"PMULTRAFFICVOLUMECS57" NUMBER(13,2), 
	"PMULTRAFFICVOLUMECS64" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPS128" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPS16" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPS384" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPS64" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPS8" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPSCOMMON" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPSSTR128" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPSSTR16" NUMBER(13,2), 
	"PMULTRAFFICVOLUMEPSSTR32" NUMBER(13,2), 
	"PMSUMRRCONLYESTABLISH" NUMBER(13,2), 
	"PMSUMSRBONLY34" NUMBER(13,2), 
	"COL_317" NUMBER(13,2), 
	"COL_318" NUMBER(13,2), 
	"PMTOTNORRCCONNECTREQSMS" NUMBER(13,2), 
	"PMNONORMALRELEASESRBONLY136" NUMBER(13,2), 
	"PMNONORMALRELEASESRBONLY34" NUMBER(13,2), 
	"COL_319" NUMBER(13,2), 
	"PMNOSYSTEMRELEASESRBONLY136" NUMBER(13,2), 
	"PMNOSYSTEMRELEASESRBONLY34" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"UTRANCELL" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   ) ;
   
   CREATE TABLE  "CLT_PM_W_ERIC_RNCFUNCTION" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"COL_350" NUMBER(13,2), 
	"PMSUMHSDCHJITTER" NUMBER(13,2), 
	"PMSAMPLESHSDCHJITTER" NUMBER(13,2), 
	"PMSUMHSEULJITTER" NUMBER(13,2), 
	"PMSAMPLESHSEULJITTER" NUMBER(13,2), 
	"PMSUMDCHDCHJITTER" NUMBER(13,2), 
	"PMSAMPLESDCHDCHJITTER" NUMBER(13,2), 
	"PMSUMHSDCHLATENCY" VARCHAR2(200), 
	"PMSAMPLESHSDCHLATENCY" VARCHAR2(200), 
	"PMSUMHSEULLATENCY" VARCHAR2(200), 
	"PMSAMPLESHSEULLATENCY" VARCHAR2(200), 
	"PMSUMDCHDCHLATENCY" VARCHAR2(200), 
	"PMSAMPLESDCHDCHLATENCY" VARCHAR2(200), 
	"PMSUMHSDCHDLRCVDELAY" VARCHAR2(200), 
	"PMSAMPLESHSDCHDLRCVDELAY" VARCHAR2(200), 
	"PMSUMHSEULDLRCVDELAY" VARCHAR2(200), 
	"PMSAMPLESHSEULDLRCVDELAY" VARCHAR2(200), 
	"PMSUMDCHDCHDLRCVDELAY" VARCHAR2(200), 
	"PMSAMPLESDCHDCHDLRCVDELAY" VARCHAR2(200), 
	"PMSUMHSDLDELAY" VARCHAR2(200), 
	"PMSAMPLESHSDLDELAY" VARCHAR2(200), 
	"PMSUMDCHDLDELAY" VARCHAR2(200), 
	"PMSAMPLESDCHDLDELAY" VARCHAR2(200), 
	"PMNOOFREDIRECTEDEMERGENCYCALLS" NUMBER(13,2), 
	"PMSENTPACKETDATA1" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANS1" NUMBER(13,2), 
	"PMTOTALPACKETDURATION1" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATION1" NUMBER(13,2), 
	"PMSENTPACKETDATA2" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANS2" NUMBER(13,2), 
	"PMTOTALPACKETDURATION2" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATION2" NUMBER(13,2), 
	"PMSENTPACKETDATA3" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANS3" NUMBER(13,2), 
	"PMTOTALPACKETDURATION3" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATION3" NUMBER(13,2), 
	"PMSENTPACKETDATA4" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANS4" NUMBER(13,2), 
	"PMTOTALPACKETDURATION4" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATION4" NUMBER(13,2), 
	"PMSENTPACKETDATAHS1" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANSHS1" NUMBER(13,2), 
	"PMTOTALPACKETDURATIONHS1" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATIONHS1" NUMBER(13,2), 
	"PMSENTPACKETDATAHS2" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANSHS2" NUMBER(13,2), 
	"PMTOTALPACKETDURATIONHS2" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATIONHS2" NUMBER(13,2), 
	"PMSENTPACKETDATAHS3" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANSHS3" NUMBER(13,2), 
	"PMTOTALPACKETDURATIONHS3" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATIONHS3" NUMBER(13,2), 
	"PMSENTPACKETDATAHS4" NUMBER(13,2), 
	"PMSENTPACKETDATAINCLRETRANSHS4" NUMBER(13,2), 
	"PMTOTALPACKETDURATIONHS4" NUMBER(13,2), 
	"PMNOOFPACKETCALLDURATIONHS4" NUMBER(13,2), 
	"PMNOIUSIGESTABLISHATTEMPTCS" NUMBER(13,2), 
	"PMNOIUSIGESTABLISHSUCCESSCS" NUMBER(13,2), 
	"PMNOIUSIGESTABLISHATTEMPTPS" NUMBER(13,2), 
	"PMNOIUSIGESTABLISHSUCCESSPS" NUMBER(13,2), 
	"COL_351" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   );
   
   CREATE TABLE "CLT_PM_W_ERIC_LOADCONTROL" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMSAMPLESMEASUREDLOAD" NUMBER(13,2), 
	"PMSUMMEASUREDLOAD" NUMBER(13,2), 
	"PMSUMSQRMEASUREDLOAD" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"EQUIPMENT" VARCHAR2(300), 
	"SUBRACK" VARCHAR2(300), 
	"SLOT" VARCHAR2(300), 
	"PLUGINUNIT" VARCHAR2(300), 
	"GENERALPROCESSORUNIT" VARCHAR2(300), 
	"LOADCONTROL" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   ) ;
   
   CREATE TABLE  "CLT_PM_W_ERIC_IURLINK" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMNOOFRLFORDRIFTINGUESPERDRNC" NUMBER(13,2), 
	"PMNONORMALRABRELEASESPEECH" NUMBER(13,2), 
	"PMNONORMALRABRELEASEPACKET" NUMBER(13,2), 
	"PMNONORMALRABRELEASECS64" NUMBER(13,2), 
	"PMNONORMALRABRELEASECSSTREAM" NUMBER(13,2), 
	"COL_348" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASESPEECH" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASEPACKET" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASECS64" NUMBER(13,2), 
	"PMNOSYSTEMRABRELEASECSSTREAM" NUMBER(13,2), 
	"COL_349" NUMBER(13,2), 
	"PMNOATTINCCNHHOCSNONSPEECH" NUMBER(13,2), 
	"PMNOATTINCCNHHOSPEECH" NUMBER(13,2), 
	"PMNOSUCCINCCNHHOCSNONSPEECH" NUMBER(13,2), 
	"PMNOSUCCINCCNHHOSPEECH" NUMBER(13,2), 
	"PMEDCHDATAFRAMESLOST" NUMBER(13,2), 
	"PMEDCHDATAFRAMESRECEIVED" NUMBER(13,2), 
	"PMEDCHDATAFRAMEDELAYIUB" VARCHAR2(200), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"IURLINK" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   );
   
CREATE TABLE  "CLT_PM_W_ERIC_IUBLINK" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMSUMDLCREDITS" NUMBER(13,2), 
	"PMSUMSQRDLCREDITS" NUMBER(13,2), 
	"PMSAMPLESDLCREDITS" NUMBER(13,2), 
	"PMSUMULCREDITS" NUMBER(13,2), 
	"PMSUMSQRULCREDITS" NUMBER(13,2), 
	"PMSAMPLESULCREDITS" NUMBER(13,2), 
	"PMDLCREDITS" VARCHAR2(200), 
	"PMULCREDITS" VARCHAR2(200), 
	"PMTOTALTIMEIUBLINKCONGESTEDDL" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"IUBLINK" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   ) ;
   
CREATE TABLE "CLT_PM_W_ERIC_GSMRELATION" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMNOATTOUTIRATHOCS57" NUMBER(13,2), 
	"PMNOATTOUTIRATHOMULTI" NUMBER(13,2), 
	"PMNOATTOUTIRATHOSPEECH" NUMBER(13,2), 
	"PMNOATTOUTIRATHOSTANDALONE" NUMBER(13,2), 
	"PMNOATTOUTSBHOSPEECH" NUMBER(13,2), 
	"COL_320" NUMBER(13,2), 
	"COL_321" NUMBER(13,2), 
	"COL_322" NUMBER(13,2), 
	"COL_323" NUMBER(13,2), 
	"COL_324" NUMBER(13,2), 
	"COL_325" NUMBER(13,2), 
	"COL_326" NUMBER(13,2), 
	"COL_327" NUMBER(13,2), 
	"COL_328" NUMBER(13,2), 
	"COL_329" NUMBER(13,2), 
	"COL_330" NUMBER(13,2), 
	"COL_331" NUMBER(13,2), 
	"COL_332" NUMBER(13,2), 
	"COL_333" NUMBER(13,2), 
	"COL_334" NUMBER(13,2), 
	"COL_335" NUMBER(13,2), 
	"COL_336" NUMBER(13,2), 
	"COL_337" NUMBER(13,2), 
	"COL_338" NUMBER(13,2), 
	"COL_339" NUMBER(13,2), 
	"PMNOOUTIRATCCATT" NUMBER(13,2), 
	"PMNOOUTIRATCCATTEUL" NUMBER(13,2), 
	"PMNOOUTIRATCCATTHS" NUMBER(13,2), 
	"PMNOOUTIRATCCRETURNOLDCH" NUMBER(13,2), 
	"PMNOOUTIRATCCRETURNOLDCHEUL" NUMBER(13,2), 
	"PMNOOUTIRATCCRETURNOLDCHHS" NUMBER(13,2), 
	"PMNOOUTIRATCCSUCCESS" NUMBER(13,2), 
	"PMNOOUTIRATCCSUCCESSEUL" NUMBER(13,2), 
	"PMNOOUTIRATCCSUCCESSHS" NUMBER(13,2), 
	"PMNOSUCCESSOUTIRATHOCS57" NUMBER(13,2), 
	"PMNOSUCCESSOUTIRATHOMULTI" NUMBER(13,2), 
	"PMNOSUCCESSOUTIRATHOSPEECH" NUMBER(13,2), 
	"PMNOSUCCESSOUTIRATHOSTANDALONE" NUMBER(13,2), 
	"PMNOSUCCESSOUTSBHOSPEECH" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"UTRANCELL" VARCHAR2(300), 
	"GSMRELATION" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   )  ;
CREATE TABLE  "CLT_PM_W_ERIC_EUL" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMEULRLCUSERPACKETTHP" VARCHAR2(200), 
	"PMEULDOWNTIMEMAN" NUMBER(13,2), 
	"PMEULDOWNTIMEAUTO" NUMBER(13,2), 
	"PMSUMEULRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESEULRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSUMEULRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESEULRLCTOTPACKETTHP" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"UTRANCELL" VARCHAR2(300), 
	"HSDSCH" VARCHAR2(300), 
	"EUL" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   )   ;
CREATE TABLE  "CLT_PM_W_ERIC_ATMPORT" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMRECEIVEDATMCELLS" NUMBER(13,2), 
	"PMTRANSMITTEDATMCELLS" NUMBER(13,2), 
	"PMSECONDSWITHUNEXP" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"TRANSPORTNETWORK" VARCHAR2(300), 
	"ATMPORT" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   ) ;
CREATE TABLE  "CLT_PM_W_ERIC_HSDSCH" 
   (	"OMCID" NUMBER, 
	"COLLECTTIME" DATE, 
	"STAMPTIME" DATE, 
	"PMNODISCARDSDUDTCHHS" NUMBER(13,2), 
	"PMNORECEIVEDSDUDTCHHS" NUMBER(13,2), 
	"PMHSDLRLCUSERPACKETTHP" VARCHAR2(200), 
	"PMHSDOWNTIMEMAN" NUMBER(13,2), 
	"PMHSDOWNTIMEAUTO" NUMBER(13,2), 
	"PMSUMHSDLRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESHSDLRLCUSERPACKETTHP" NUMBER(13,2), 
	"PMSUMHSDLRLCTOTPACKETTHP" NUMBER(13,2), 
	"PMSAMPLESHSDLRLCTOTPACKETTHP" NUMBER(13,2), 
	"MANAGEDELEMENT" VARCHAR2(300), 
	"RNCFUNCTION" VARCHAR2(300), 
	"UTRANCELL" VARCHAR2(300), 
	"HSDSCH" VARCHAR2(300),
	"RNCNAME" VARCHAR2(50)
   )