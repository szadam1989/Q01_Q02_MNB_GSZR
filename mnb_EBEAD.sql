--@q:\mnb_ebead_70.sql
--@d:\work\Megrendelesek\MNB\mnb_EBEAD_70.sql
/*
MNB napi v�ltoz�slista k�ld� program vb_app_init verzio
2.1 v�ltozat
--2.0:A lek�rdez�s felt�tele: b�rmelyik rendszerbe ker�l�si d�tummal ell�tott mez� 
a megadott sz�m� nappal visszamen�leg 
t�rt�nt v�ltoz�sa eset�ben lek�rdez�sre ker�l.
--  2.1 :Ha nincs lev�logatand� rekord, a mell�kletbe akkor is ker�l egy-egy el�re defini�lt rekord.
--      a lev�logat�s a trunc(sysdate) �s a trunc(sysdate-1) k�z�tt t�rt�nik.
--      kivettem az alakul�s d�tum�nak figyel�s�t.
--      2010. janu�r. 20
-- 2.2: Ha az ESA szektork�d �res, '90'-es k�dot ad �t a program. Az ESA k�d hat�lya ekkor az alakul�s d�tuma
--      Egy hiba is jav�t�sra ker�lt:  az egyetlen "�res " rekordot tartalmaz� 'Q1' -es mell�kletbe is 'Q2' ker�lt be, ez jav�tva 'Q1'-re.
--      2010 febru�r 22.
-- 2.3: Hib�s volt a d�tumok megad�sa  a trunc(sysdate-1) between trunc(sysdate) az el�z� verzi�ban a 
--        tegnap hajnalban t�rt�nt v�ltoz�sokat, �s a ma hajnalban (a lek�rdez�s el�tt) t�rt�nt v�ltoz�sokat is 
--        elk�ldte. Azokat a v�ltoz�sokat ellenben, amelyek a fut�sa ut�n t�rt�ntek, (nap k�zben pl a gy�ri igazgat�s�g m�dos�t�sai) m�r csak egyszer, a k�vetkez� napon k�ldte el.
--        a d�tum intervallumok als� hat�r�t most v�ltoz�s d�tum<=trunc(sysdate) re �ll�tottam: ker�ljenek bele a tegnapi v�ltoz�sok,
--                                               fels� hat�r�t       v�ltoz�s d�tum<trunc(sysdate) re �ll�tottam: ne ker�ljenek bele a mai napi v�ltoz�sok 
--        A between-nel az el�z� verzi�ban a tegnapi �s mai v�ltoz�sok is beker�ltek minden nap, �gy g�rd�l�, k�t napi v�ltoz�sokat adtunk �t.
--  3. :  Ha valami miatt nem fut le a program, neh�z az el�z� v�ltoz�sok megtal�l�sa. Ez a verzi� elmenti az el�z� fut�s d�tum�t, �s az az�ta bedolgozott 
--         utols� v�ltoz�sokat k�ldi el, akkor is, ha ez m�r t�
--  4.    K�t spool file-t is k�sz�t, ha nem tudn� fogadni a levelet egyik postal�da sem.
--   5.    K�t spool file-t k�sz�t naponta, a nevet a d�tumb�l gener�lva, �gy tetsz�leges sz�m� riport �rizhet� meg visszamen�leg
--   6.    2010. okt�ber 16.  
--                 csak az �l� �s t�rgy�vben megsz�nt szervezeteket v�logatja
--                 a vb_rep.gszr_mnb_napi t�bl�ba sz�rogatja be a naponta elk�ld�tt rekordokat.
-- 7. Ha k�lf�ldi sz�khely�, akkor nem kezd�dhet 23-mal a GFO-ja: nem lehet egy�ni v�llalkoz�, illetve nem lehet a j�v� �vi GFO 961 vagy nem kezd�dhet 23-mal
-- 8. 2011. jan. 5.: a GFO v�lt�s miatt az m049 helyett az m0491-es nomenklat�r�t k�ldj�k. A lrev�logat�sban is az m0491-es nomenklat�r�t figyelj�k.
-- 9. 2011.jan 24.: azokr�l a c�gekr�l, amelyek a t�rgy�vet megel�z�en sz�ntek meg, de csak t�rgy�vben ker�lt be inform�ci� a regiszterbe, k�ldj�n �rtes�t�st.
-- 10. 2011. m�jus 5. kihagytam a k�lts�gvet�sieket. Ez�rt a t�rzssz�m szerinti sz�r�st ki kellett venni.
-- 12. 2011. j�nius 22: a k�s�r� lev�l tartalmazza a lev�logat�s indul� �s z�r�  id�pontj�t.
-- 13. 2011 november 17. a k�lf�ldi sz�khely� c�gek nincsenek kiz�rva a tov�bbiakban.
-- 14. 2012. m�jus 21. Az egyedileg k�rt t�rzssz�mokat hozz�csapja a lek�rdez�shez a vb_rep.vb_app_init t�bla m003 param�tereib�l szedve 
--       A fut�sid�h�z k�pest 1 �r�val kor�bbi id�ponttal besz�rt (datum_tol)  t�rzssz�mokat csapja a napi list�hoz
-- 15. 2013.janu�r 31. Kivettem a k�lf�ldi sz�khely� szervezeteket kiz�r� felt�telt
-- 15. 2013. m�rcius.25    A program egy �j, a vb_rep.mnb_napi nev� t�bl�ba �rja a mell�kletek nev�t, rekordjait stringk�nt, �s a rekordok sorsz�m�t
-- 16. 2013. �prilis 3. A sorsz�m mez� numerikus.
--  17. 2013. �prilis 3. Kivettem a pontot a filen�vb�l
--  18  2013 �prilis 9. �sszeveti az �rbev�tel- �s l�tsz�m kateg�ria k�dokat a kurzorban a hist2 m025,m026 utols� elmentettj�vel, �s kihagyja azokat a rekordokat, ahol k�dv�ltoz�s nem t�rt�nt.
--         Ha naponta t�bbsz�r fut, a kurzor sorsz�m mindig 1-el kezd�d� rekord sorsz�mot gener�l. Ez�rt fut�s el�tt lek�rdezem a maxim�lis sorsz�mot az adott napra. 
--   19  EBEAD   a lev�logat�s d�tuma essen 1900.01.01 el�tti �s a 2499.12.31  k�z�
--       az utols� sikeres felt�lt�sn�l r�gebbi adatokat kit�rli a t�bl�b�l.
--       csak akkor ad �t adatokat, ha azok k�z�l valamelyik mez� nem azonos a m�r �tadottal, akkor is, ha d�tumban frissebb.
--  20. 2013. okt. 3. hibalist�ra teszi a hib�s hat�lyd�tumokat a k�perny�re �r, illetve az email sz�veg�ben elk�ldi azokat.          
--       2013. okt�ber 9. ha egy nap t�bbsz�r fut, lekezeli, ha m�r t�lt�tt be rekordokat: a sorsz�moz�st folytatja.
--             Lekezeli, ha nem j�k a hat�lyd�tumok, azokr�l hibajegyz�ket k�sz�t.
--             nem k�ld rekordot, ha csak az ESA szektork�d, �rbev�tel vagy l�tsz�m v�ltozott, de a v�ltoz�s nem v�ltoztatta meg ezeket a k�dokat (azaz maradtak a kateg�ri�ban),
--              �s m�s v�ltoz�s pedig nem t�rt�nt
--       a debug_on:true �ll�t�s�val nem m�dos�tja a vb_app_init t�bl�ban az utols� k�ld�s d�tum�t.
--       odafigyel arra, hogy az utols� felt�lt�sn�l r�gebbi rekordokat t�r�lje, ne az utols� lev�logat�sn�l r�gebbieket.
-- 21. 2014. febru�r 5. az m025-�t �s m026-ot nvl-eztem.
--       kivettem a megsz�nt szervezetek azonos �vi hat�ly�t vizsg�l� felt�telt
--       betettem sz�k�t�sk�nt az m0491!='811' -et
-- 22. 2014. m�rcius 18.
--  Ha az �rbev�tel hat�lya �res, az alakul�s d�tum�val t�lt�m f�l.
-- 23. 2014. m�rcius 25. 
-- a list�ban megadott t�rzssz�mokat sz�mm� konvert�lom, valamint uni�t csin�lok a kurzorban
-- 24. debug k�rnyezetet �s debug kapcsol�t tettem bele.
-- az alter session bekapcsol�sa ut�n nem hajt v�gre inserteket �s a vb_app_init t�bl�ban az
-- mnb_EBEAD.sql - debug program n�vvel ell�tott param�tereket  kezeli
-- 25. 2014. m�jus 14,
-- a nemzeti sz�ml�s TE�OR-t v�logatja le, ha olyan l�tezik.
-- 25. 2014.- j�lius 11.
-- minden fut�s sor�n v�gign�zi az �sszes nem nemzeti sz�ml�s rekordot, hogy meg�llap�tsa, 
-- a lez�r�s �ta t�rt�nt-e v�ltoz�s a stat f�tevben. 
-- Ha t�rt�nt, lez�rja. 
-- Tesztelve a tesztdb-n 2014. j�l. 14.
-- 26. 2014. augusztus 14.
-- ha a nemzeti sz�ml�s te�or le van z�rva, a lez�r�s d�tuma lesz a k�ld�tt stat TE�OR hat�lya.
-- 30. 2014. nov. 11.
--  nemzeti sz�ml�s TE�OR-ok hat�ly�nak kalkul�l�sa pontos�t�sa
-- 31. �sszedolgoztam a pr�ba-funkci�s verzi�t a nemzeti sz�ml�s pontos�t�sokkal
-- let�roltam a f�ggv�nyt vb_rep alatt: vb_rep.SPEC_CHAR_DUP(SZO IN VARCHAR2, HOSSZA IN NUMBER, SPEC_CHAR IN VARCHAR2) 
-- tesztelve 2014.12.02. tesztadatb�zison j�
-- 32. Ha kiker�l egy szervezet a nemzeti sz�ml�k mnegfigyel�si k�r�b�l, a jelenlegi stat TE�OR mell�
--        lev�logatjuk a stat TE�OR id�sor�t is a k�rbe val� beker�l�st�l kezd�d�en
--        dbms_outputra logfileba megy, de tehet� �j rekordt�pusk�nt a t�bl�ba is.
-- 44. hib�t jav�tottunk a nemzeti sz�ml�s TE�OR-okkal kapcsolatban: a t�rzsadat �llom�ny lev�laogat�s�hoz  is le kellett sz�rni a k�ldend�
--     rekordot, k�l�nben elk�ldte m�dos�t�s eset�n k�tszer.
-- 45. z�r�jelez�si szintaktikai hiba jav�tva 
-- 46. m�g maradt szintaktikai hiba, az is jav�tva
-- 2015. nov. 9.
-- 48. a k�l�n list�n k�rt rekordok �s a nemzeti sz�ml�s te�orok anom�li�inak m�dos�t�sa a k�l�n list�s kurzor r�sz where felt�tel�ben
-- 50. rekordsorsz�mot vezettem be, a ciklusban l�ptetve, mert ha egy rekordot a GSZR-b�l �s a nemzeti sz�ml�sb�l is
--       be kell tenni , a lek�rdez�sben kapott eredm�nyben k�t k�l�nb�z� rownum j�n le, �gy duplik�lt t�rzssz�mokat ad
-- 2016. 05.23 A nevet 250 karakter maxim�lis hosszban adhatjuk.
-- 51. �jra szerveztem a programot: ha egy rekordban az F003-ban is �s a nemzeti sz�ml�s t�bl�ban is t�rt�nt v�ltoz�s, vagy ha egy 
--    nemzeti sz�ml�s rekordot t�rzssz�m szerint kellett elk�ldeni, �s a k�t TE�OR nem volt azonos, az UNION nem ejtette ki 
--     az egyik rekordot, a k�l�nb�z� TE�OR-ok miatt. �gy is duplik�lt t�rzssz�mokat kaptam. A kurzor most csak t�rzssz�mokat k�rdez
--     le.
-- 52. besorsz�moztam az eddig v�ltoz�val. Ha hiba�zenet van, ki�rja, meddig jutott a program.
-- 53. 2017.03.30. a hat�ron �tny�l� megsz�n�sek jelz�se  
--   ha megsz�nt a szervezet (0,vagy 9 k�d), megn�zni szerepel-e a vb_ceg.jogutod t�bl�ban hogy m003=m003_je
--    amennyiben ott kulf_ju=1 akkor a 15-�s mez�be 1-et �rni, egy�bk�nt nem szabad rekordot sem �tadni a q02-ben.
-- 54. 2017.07.17. Ha t�bb napra kiesik a felt�lt�s, akkor, ha egy adott napon nem volt v�ltoz�s, nem k�pz�d�tt puffer rekord sem a q01-b�l, sem a q02-b�l.   
--    ezt jav�tottam azzal, hogy nem az MNB_napi t�bl�ban l�v� q01 ill. q02 rekordok sz�m�t, hanem a sysdate-val azonos d�tum� q01 ill. q02 
--    rekordok sz�m�t k�rdezem le.
-- 55. 2017. augusztus 15. B�v�lt a jeju rekordok form�tuma �s lev�logat�suk. A jogut�dok lev�logat�sakor a jogut�d n�lk�l megsz�nt szervezetek eset�ben 
--    lek�rdezem a vb_ceg.jogutod t�bl�j�t is. Amennyiben ott tal�lok a t�rzssz�mra rekordot, hogy m003=m003_je, lev�logatom az ottani rekordot is.
--    A jeju rekordok k�lf�ldi jogut�d eset�n a 00000001 t�rzssz�mot kapj�k jogut�d t�rzssz�mk�nt. A j. �talak nev� mez� tartalm�t �tk�dolom v�ltoz�s k�dra.
--    Az �tadott jeju rekordba pedig beker�lt kett� �j mez�: a k�lf_ju (belf�ldi jogut�d eset�n '0', k�lf�ldi jogut�d eset�n '1'), illetve az orsz�gk�d 
--    mez�, de ez jelenleg mind belf�ldi, mind k�lf�ldi jogut�d eset�ben null �rt�k. Fenntartva a vb_ceg.jogutod t�bla orsz�gk�d mez�vel t�rt�n� 
--    b�v�t�s�re.
--    az �talakul�s m�dja k�d �tk�dol�sa:
--    decode(j.atalak,'A','220','B','230','O','240','V','280','K','830','230') mv501_je, a jogel�d
--	  decode(j.atalak,'A','120','B','930','O','140','V','180','K','130','930') mv501_ju, illetve a jogut�d v�ltoz�sk�dja tekintet�ben
--56. 2018.01.09. a d�tumok hat�rellen�rz�se sor�n a hib�s d�tumok ki�r�sa t�puskonverzi�s hib�s volt.
--     Hogyan m�k�d�tt m�gis m�ig?
--57. 2018.01.16. statisztik�k gy�jt�se a ciklusban: melyik attributum h�ny rekordon v�ltozott, ami�rt lev�logat�sra ker�lt a rekord?
--    az egyes v�ltoz�sok sz�m�b�l le kell vonni az �jonnan beker�lt t�rzssz�mok sz�m�t.
--58.  2018.02.14. D�tumkonverzi�s hiba jav�tva 28-n�l.
-- az 59.-es �s 60.-as v�ltoztat�s m�gsem kellett: ez a c�mregiszter illetve a megsz�nt szervezetekre �rkez� m�dos�t�s
-- kisz�r�se lett volna.
--59. Ha a KSH vagy az MNB adatait k�ld�m, akkor 1983.01.01 legyen az alakul�s d�tuma.
--61.  A c�gjegyz�k sz�m, valamint az mvb39 (ifrs-nyilatkozat) �tad�sa �s v�ltoz�sfigyel�se.
--62.  2019.01.15. A log file ki�rat�s�nak kis m�dos�t�sa: ki�rja a m�g el nem k�ld�tt rekordok sz�m�t naponta.
--63.  2019.02.11. A jogel�d-jogut�d v�ltoz�si k�dok �tk�dol�sa megv�ltozott.
--64.  2019.05.21. A debug kapcsol�hoz hozz� tettem a k�l�n t�rzssz�mok lev�logat�s�t is. Ezeket mnb_EBEAD_debug programn�vvel kell bet�lteni
--          �gy megoldhat�, hogy a napi lev�logat�sok zavar�sa n�lk�l az mnb_napi_debug t�bl�ba 
--          k�l�n list�t v�logassunk le, amelyet text file-ba spoololva f�l lehet t�lteni. 
--          figyelni kell az id� param�terez�sre: a programot a param_dtol mez� �rt�k�nek megfelel� id�ponthoz min�l 
--          k�zelebb kell ind�tani, �s az utols�_futas_debug param�tert pedig k�zvetlen�l a program ind�t�sa el�tti id�pontra 
--          kell �ll�tani. A programot pedig debug on:true m�dban ind�tani Ekkor a vb_rep.mnb_napi_debug t�bl�ba v�logat.
--          Ha k�l�n list�n k�rik a t�rzssz�mot, akkor is lev�logatja, ha m�r az aktu�l�s �vn�l r�gebben sz�nt meg.
--65.  2019.05.27. A jeju rekord kib�v�lt egy egy karakteres, utols� mez�vel: Amennyiben a dig ki van t�ltve a f003_jeju rekordban, a
--          mez� �rt�ke '1', egy�bk�nt '0'. A k�lf�ldi jogut�d vb_ceg.jogutod eset�ben mindig '0', ott nincs lez�r�si inform�ci�.
--66.  2019.08.26. Ha az alakul�s d�tuma ut�lag m�dosul, az f003_hist3pr lev�logat�s�val a t�rzssz�m beker�l a kurzorba
--67.  2019.09.06.  Levizsg�ljuk, hogy megv�ltozott-e az alakul�s d�tuma ut�lag, hogy ha m�s nem v�ltozott, akkor is benne maradjon a 
--           lev�logat�sban
--68.  2020.04.02. Ha a vb_rep.vb_app_init-be '-m003' param�terrel �runk be t�rzssz�mot, azt kihagyja a lev�logat�sb�l.
--69. 2020.09.02.  Finom�tottam a mehet �s a megsz�nt seg�ts�g�vel, ne k�ldj�nk megsz�ntekr�l jelent�st a "m�gis mehet" seg�ts�g�vel.
--70. 2021.02.08. Ha �ll a szerver (pl. karbantart�s miatt), vagy m�s�rt nincs lek�rdez�s,
--    az utols� fut�s �ta eltelt napokra "�res" �zeneteket gener�l.
*/
set termout on
set linesize 1000
set trimspool on
--set pagesize 0
set heading off
set echo off
set serveroutput on size 1000000

alter session set plsql_ccflags='debug_on:false';

DECLARE
verzio varchar2(25):=' 70. verzi�';
    PLUSZ_Q01        number:=0; 
    ALAKDAT_MODSZAM  number:=0; 
    M003_ALAKDAT_CHANGED varchar2(8 char);
    sorok            number:=0;   
    sorok1           number:=0;
	puffersorok      number:=0;
    datum_kar            varchar2(21);
    datum            date;         
    utolso_futas     date;    
	utolso_kuldes    date;
    w_cdv            varchar2(1);
    w_m025           varchar2(2):='  ';
    w_m026           varchar2(1):=' ';
   -- w_m063           varchar2(2):='  ';
    wm0581 varchar2(4):=null;
    sor              varchar2(1000);
    q01db            number;
    q02db            number;
    q01mellekletnev  varchar2(30);
    q02mellekletnev  varchar2(30);
    napok_szama_visszamenoleg number:=1;  -- alap�rtelmezett a tegnapi nap �ta
    serr             number:=0;               --hibak�d
    errmsg  varchar2(100);                 -- hiba�zenet
    alsohatar  date :=to_date('19000101','YYYYMMDD');
    felsohatar date :=to_date('24991231','YYYYMMDD');
    kihagyott  number:=0;
    ures_kuldes number:=0;
    mehet boolean:=false;
    megszunt boolean:=false;
    levalogatva_1 number:=0;
	levalogatva_2 number:=0;
    nem_valtozott number:=0;
	osszesq01 number:=0;
    kulondb number:=0;      --a k�l�n list�n k�rt t�rzssz�mok sz�ma
    programneve varchar(40);
    tablaneve varchar2(40):=$if $$debug_on $then 'vb_rep.mnb_napi_debug'; $else 'vb_rep.mnb_napi'; $end
    w_statteaor varchar2(4);
    w_stathataly date;
	w_hatalyvege date;
	w_stat_r date;
	w_kuldes_vege date;
	w_m003 number;
	w_alakdat date:=null;
    uzenet varchar2(60):='';
    lezarasdb number:=0;
    rownumber number;
    maxm0581r date;
    rekordsorszam number:=0;
--konstansok
    nev_hossz number:=250;  --2016.05.23.-t�l, m�r az 50-es verzi�ban is.
	rnev_hossz number:=250;  --az okozza az elt�r�st, hogy a r�vid nevet eddig nem v�gtam le!
	utca_hossz number:=80;
	telnev_hossz number:=20;
	pfiok_lev_hossz number:=20;
	regi_datum date:=to_date('19000101','YYYYMMDD'); 
	
	m003_r_db       number:=0;
	m005_szh_db     number:=0;
	nev_r_db        number:=0;
	rnev_r_db       number:=0;
	szekhely_r_db   number:=0;
	levelezesi_r_db number:=0;
	LEV_PF_R_db     number:=0;
	m040k_r_db	    number:=0;
	m040v_r_db      number:=0;
	letszam_h_db    number:=0;
	arbev_r_db      number:=0;
	m0781_r_db      number:=0;
	m0581_r_db      number:=0;
	MP65_r_db       number:=0; --m063_r_db
	ueleszt_db      number:=0;
	m0491_r_db      number:=0;
	cegv_db         number:=0;
	ifrs_db         number:=0;
    m003_r_db_for_rnev       number:=0;
    m003_r_db_for_levelezesi       number:=0;
    m003_r_db_for_lev_pf       number:=0;
    m003_r_db_for_MP65       number:=0; --m003_r_db_for_m063
    m003_r_db_for_cegv       number:=0;
	cursor gszr_cur (utso_futas date, programneve_1 varchar) is  select rownum rn, m003 from
	                (select m003 from vb.f003 where 
	                     (greatest(nvl(m003_r,regi_datum),
						          nvl(m005_szh_r,regi_datum),
								  nvl(nev_r,regi_datum),
								  nvl(rnev_r,regi_datum),
								  nvl(szekhely_r,regi_datum),
								  nvl(levelezesi_r,regi_datum),
						          nvl(LEV_PF_R,regi_datum),      
						          nvl(m040k_r,regi_datum),
								  nvl(m040v_r,regi_datum),
								  nvl(letszam_R,regi_datum),--Szil�gyi �d�m 2022.10.19. _R _h helyett
								  nvl(arbev_r,regi_datum),
								  nvl(m0781_r,regi_datum),
								  nvl(m0581_r,regi_datum),
								--  nvl(MP65_r,regi_datum), --m063_r --Szil�gyi �d�m 2023.09.27.
								  nvl(ueleszt_R,regi_datum),--Szil�gyi �d�m 2022.10.18. _R
								  nvl(cegv_r,regi_datum),
								  nvl(mvb39_r,regi_datum)
								  )>= utso_futas
								  or (m0491_r>= utso_futas and m0491_f!='06')
                         )						
                     and
                        substr(m0491,1,2)!='23' and m0491!='961' and m0491!='811'
                    union
                        select m003 from 
						vb.f003_m0582
						where (nvl(m0582_r,regi_datum)>=utso_futas)
					union
                        select m003 from vb.f003_hist3pr where datum>utso_futas and alakdat!=alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811')			
	                union				   
                        select m003 from vb_rep.vb_app_init where 
							   program='mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS')>utso_futas
							       and param_nev='m003'
					minus
                        select m003 from vb_rep.vb_app_init where	--ezeket a t�rzssz�mokat kihagyja az adatk�ld�sb�l (egyszer)				
							   program=programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS')>utso_futas
							       and param_nev='-m003' 
							   );

						
	TYPE gszr_table IS TABLE OF gszr_cur %ROWTYPE
    INDEX BY PLS_INTEGER;
	gszr_rec gszr_cur%rowtype;				   
    Type q01_rec_type is record (Q01 varchar2(3 char),                        -- adatgy�jt�s k�dja
	                             datum1 varchar2(8 char),                    -- vonatkoz�si id� 8 hosszon
                                 KSH_torzsszam varchar2(8 char),             -- KSH t�rzssz�ma
                                 kitoltes_datum varchar2(8 char),            -- kit�lt�s d�tuma 8 hosszon
                                 kshtorzs varchar2(20 char),                 -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
                                 rekordsorszam varchar2(7 char),             -- sorsz�m 7 karakteren el�null�zva  a kurzor rekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
                                 torzsszam varchar2(8 char),                 -- t�rzssz�m
                                 gfo varchar2(3 char),                       -- gazd.forma
								 gfo_hataly varchar2(8 char),                -- gfo hat�ly d�tuma
								 megyekod varchar2(2 char),                  --sz�khely megyek�dja, 
                                 megyekod_hataly varchar2(8 char),           --a megyek�d hat�ly d�tuma
                                 nev   varchar2(250 char),                   --n�v
                                 nev_h varchar2(8 char),                     --n�v hat�lya
                                 rnev  varchar2(250 char),                    --r�vid n�v
                                 RNEV_H varchar2(8 char),                    --r�vid n�v hat�lya
                                 M054_SZH varchar2(4 char),                  --sz�khely ir�ny�t� sz�m
                                 telnev_szh varchar2(30 char),--20 volt Szil�gyi �d�m
                                 utca_szh varchar2(100 char),--80 volt Szil�gyi �d�m
                                 SZEKHELY_H varchar2(8 char),                --sz�khely c�m hat�lya            
                                 M054_LEV  varchar2(4 char),                 --levelez�si c�m ir�ny�t� sz�ma
                                 telnev_lev   varchar2(30 char),--20 volt Szil�gyi �d�m
                                 utca_lev varchar2(80 char),                             
                                 LEVELEZESI_R varchar2(8 char),              --levelez�si c�m hat�lya (rendszerbe ker�l�se)
                                 m054_pf_lev varchar2(4 char),               -- postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                                 telnev_pf_lev varchar2(30 char),  --20 volt Szil�gyi �d�m          -- postafi�kos levelez�si c�m telep�l�s neve
                                 pfiok_lev varchar2(10 char),                 -- postafi�k
                                 leV_PF_R varchar2(8 char),                  -- pf. c�m hat�lya (rendszerbe ker�l�se)
                                 M040 varchar2(1 char),                      -- m�k�d�s �llapotk�dja 
								 m040k varchar2(8 char),                     -- �llapotk�d hat�lya
                                 mukodv varchar2(8 char),                    -- m�k�d�s v�ge
                                 m025 varchar2(2 char),                      -- l�tsz�m kateg�ria
                                 letszam_h varchar2(8 char),                 --l�tsz�m kateg�ria besorol�s d�tuma
                                 m026 varchar2(1 char),                      --�rbev�tel kateg�ria
                                 arbev_h varchar2(8 char),                   --�rbev�tel kateg�ria hat�lya
                                 m009_szh varchar2(5 char),                  --sz�khely telep�l�s k�d+cdv 5 jegyen
                                 alakdat varchar2(8 char),                    --alakul�s d�tuma
                                 m0781 varchar2(4 char),                     --admin szak�g 2008
                                 m0781_h varchar2(8 char),                   --2008-as besorol�si k�d hat�lya
                                 m058_j varchar2(4 char),                    --janus TE�OR
                                 m0581_j_h varchar2(8 char),                 --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                                 MP65 varchar2(5 char),   --m063 varchar2(2) --ESA szektork�d
                                 MP65_H varchar2(8 char),       --m063_h     --ESA szektork�d hat�lya
                                 ueleszt   varchar2(8 char),                 --�jra�leszt�s hat�lya
                                 m003_r varchar2(8 char),                    --rendszerbe ker�l�s d�tuma
                                 datum2 varchar2(8 char),                     --napi lev�logat�s d�tuma
                                 statteaor varchar2(4 char),                 --statisztikai TE�OR
                                 stathataly varchar2(8 char),                --statisztikai TE�OR hat�lya
								 cegjegyz   varchar2(20 char),                --c�gjegyz�k sz�m
								 cegjegyz_h  varchar2(8 char),               --c�gjegyz�ksz�m hat�lya
								 mvb39       varchar2(1 char),               --IFRS nyilatkozat
								 mvb39_h     varchar2(8 char),                --IFRS nyilatkozat hat�lya
                                 ORSZ varchar2(2 char),
                                 LETSZAM varchar2(6 char),
                                 ARBEV varchar2(8 char),
-- rendszerbe ker�l�sek a feldolgoz�shoz
                                 m003_r_date date,
								 m0491_r  date,
								 m005_szh_r date,
								 nev_r date,
								 rnev_r date,
								 szekhely_r date,
								 m040k_r date,
								 m040v_r date,
								 m0781_r date,
								 m0581_r date,
								 MP65_r date, --m063_r
								 arbev_r date,
								 levelezesi_r_date date,
								 lev_pf_r_date date,
                                 letszam_h_date date,
								 cegjegyz_r date,
								 mvb39_r date,
                                 UELESZT_R date--Szil�gyi �d�m 2022.10.19.
	                            );
    mnb_rec q01_rec_type;
	nsz_db number;                                                      -- h�ny nemzeti sz�ml�s te�or-rekordja van a szervezetnek?
	sqlmsg varchar2(50);
	eddig number:=0;
	kulf_db number:=0; --hat�ron �tny�l� megsz�n�sek hat�ron t�li jogut�djainak darabsz�ma

	
BEGIN
	programneve:= $if $$debug_on $then 'mnb_EBEAD_debug.sql' $else 'mnb_EBEAD.sql' $end;
	$if $$debug_on $then dbms_output.put_line('DEBUG m�d bekapcsolva'); $end
    
    --dbms_output.put_line('MNB napi GSZR v�ltoz�s lev�logat�: ' || programneve || verzio);
	
    select to_char(sysdate,'YYYY-MM-DD HH24:MI:SS'),sysdate into datum_kar,datum from dual;
    --tesztel�shez
--      update vb.vb_mod_szam set kezdet='2009-01-01 08:51:25' where usernev='K865' and sqlnev='mnb.sql';
--      commit;
      --dbms_output.put_line('elindult');
      --dbms_output.put_line('Most �rom a vb_mod_szam-ot');
      -- insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','1. Megvolt a vb_mod_szam �r�sa');
      -- commit;
--teszt v�ge  
    eddig:=1;      
	begin
	  $if $$debug_on $then
	    vb.mod_szam_tolt('K865','vb_rep.mnb_napi',sorok,'MNB napi v�ltoz�slista k�ld�se,debug m�d ',programneve||verzio,datum,'K');
	  $else
	    vb.mod_szam_tolt('K865','vb_rep.mnb_napi',sorok,'MNB napi v�ltoz�slista k�ld�se',programneve||verzio,datum,'K');
	  $end
        commit;
    exception when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||datum_kar,'mod_szam_tolt a hiba');
        commit; 
    end;
       -- insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','2. param�ter �rt�kek kiszedve');
        --commit;
	eddig:=2;
--	dbms_output.put_line('eddig j�');
    begin        
      --  $if $$debug_on $then
       --     select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
        --            where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
         --            and   program='mnb_kuld.sql'
          --           and param_nev='utolso_futas - debug';			
       -- $else
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
                     and   program='mnb_kuld.sql'
                     and param_nev='utolso_futas';
       -- $end                                            
    exception when others then 
	    dbms_output.put_line(sqlerrm);
		dbms_output.put_line('A vb_app_init t�bl�b�l az utols� k�ld�s d�tum�t nem lehetett olvasni');
        utolso_kuldes:=sysdate-1;
    end; 
    --dbms_output.put_line('Utols� felt�lt�s: ' || to_char(utolso_kuldes, 'YYYY-MM-DD HH24:MI:SS') || '.');	
	eddig:=3;
 --  Mikor futott utolj�ra a lek�rdez�s?
    begin        
        $if  $$debug_on $then
            --dbms_output.put_line('utolso_futasba-debug');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas - debug';
        $else
            --dbms_output.put_line('utolso_futasba-�les');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se'
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas';
        $end                                            
    exception when others then 
	--dbms_output.put_line('itt sincs adat');
        utolso_futas:=sysdate-1;
    end;    
/*
    if utolso_futas is null then 
        utolso_futas:=sysdate-1; 
    end if;        
*/
    dbms_output.put_line('Utols� lev�logat�s: ' || to_char(utolso_futas, 'YYYY-MM-DD HH24:MI:SS') || '.');	
    dbms_output.put_line('Utols� felt�lt�s: ' || to_char(utolso_kuldes, 'YYYY-MM-DD HH24:MI:SS') || '.');	
	eddig:=4;
      
--t�r�lni az utols� sikeres felt�lt�sn�l r�gebbi rekordokat
     begin
            delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where 
			substr(rekord,5,8)<=to_char($if $$debug_on $then utolso_futas $else utolso_kuldes $end,'YYYYMMDD');
            commit;
     exception when others then  
            serr:=sqlcode;           
            insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld� '||programneve||datum_kar, 'r�gi elk�ld�tt rekord t�rl�s hiba'||to_char(utolso_kuldes,'YYYYMMDD'));
            commit; 
     end; 
     eddig:=5;
--   rekordt�pus nev�nek �sszerak�sa(a mell�kletnevek �sszerak�sa )      
    q01mellekletnev:='Q01'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
    q02mellekletnev:='Q02'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
       
--t�r�lni az esetleges �res k�ldend�ket, hogy ne legyen 2 els� sor!
    begin
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q01mellekletnev=filename and substr(rekord,32,1)='N';
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q02mellekletnev=filename and substr(rekord,32,1)='N';
        commit;
    exception when others then    
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld� '||programneve||datum_kar,'puffersor t�rl�s hiba');
        commit; 
    end; 
    commit;
    --dbms_output.put_line('T�r�ltem a puffer sorokat.');
    --dbms_output.put_line('T�r�ltem az utols� sikeres felt�lt�sn�l r�gebbi rekordokat.');
    eddig:=6;
    
    begin
                    select count(*) into PLUSZ_Q01
                    from vb_rep.vb_app_init where 
							   program='mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS')>utolso_futas
							       and param_nev='m003';
                     if (PLUSZ_Q01 > 0) then
                        dbms_output.put_line('A Q01-es adat�tad�ssal kik�ldend� plusz c�gek sz�ma: ' || PLUSZ_Q01 || '.');
                     end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kiv�tel t�rt�nt a plusz c�gek sz�m�nak meghat�roz�sa sor�n.');
                end;
                eddig:=120;
                
                
    begin
                    select count(*) into ALAKDAT_MODSZAM
                    from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811');
                   --  if (ALAKDAT_MODSZAM > 0) then
                        --dbms_output.put_line('Az alakul�s d�tuma megv�ltozott a k�vetkez� sz�m� esetekben: ' || ALAKDAT_MODSZAM || '.');
                    -- end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kiv�tel t�rt�nt az alakul�s d�tum�nak v�ltoz�s�nak sz�m�nak meghat�roz�sa sor�n.');
                end;
                eddig:=130;
 

 --k�ld�s v�ge azokra a lez�rt k�ldend� m0582-kre, amelyekre az el�z� fut�s �ta megv�ltozott az m0581, �s az nem egyenl� az m0582-vel
    for i in (--select t.rowid rn,t.m003 from vb.f003 g, $if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
	           select t.rowid rn,t.m003 from vb.f003 g,  vb.f003_m0582 t where
                  t.m003=g.m003 and m0582_hv is not null and t.kuldes_vege is null
                  and g.m0581_r>=utolso_futas --stat TE�OR m�dosult
                  union
                  --select t.rowid rn,t.m003 from vb.f003 g,$if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
				  select t.rowid rn,t.m003 from vb.f003 g, vb.f003_m0582 t where 
                  t.m003=g.m003 and t.m0582_r>=utolso_futas and t.kuldes_vege is null)   --NSZ TE�OR (vagy hat�lya) m�dosult
    loop
        begin   
				$if $$debug_on $then 
				    null;
				$else	
				update  vb.f003_m0582 
					set kuldes_vege=datum,
						   kuldendo='0'
					where i.rn=rowid;
				$end	
        exception
            when others then
                serr:=sqlcode;
                INSERT INTO VB.VB_UZENET VALUES(serr,programneve||datum_kar,'Valami g�z van a lez�r�ssal:'||TO_CHAR(I.M003));
                commit;
                rollback;               
        end; 
		eddig:=161;
        sorok:=sorok+1;               
        if mod(sorok,100)=0 then
            begin
			$if $$debug_on $then
                vb.mod_szam_tolt('K865','vb_rep.mnb_napi',sorok,'MNB napi v�ltoz�slista k�ld�se,debug m�d ',programneve||verzio,datum,'M');
			$else
			    vb.mod_szam_tolt('K865','vb_rep.mnb_napi',sorok,'MNB napi v�ltoz�slista k�ld�se',programneve||verzio,datum,'M');
			$end
            exception when others then
                insert into vb.vb_uzenet values (serr,programneve||datum_kar,'mod_szam_tolt a hiba');
            end;
            commit;
        end if;                 
    end loop;
    if(sorok > 0) then
        dbms_output.put_line('Nemzeti sz�mla TE�OR k�d v�ltoz�sok sz�ma: ' || to_char(sorok) || '.');
    end if;
	eddig:=7;
    sorok:=0;
    nsz_db:=0;
    begin
        select nvl(max(sorszam),0) into sorok from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q01mellekletnev=filename and substr(rekord,32,1)='E';
	exception
        when no_data_found then 
            sorok:=0;
	end; 
    
	--dbms_output.put_line('A t�bl�ban tal�lhat� - m�r lev�logatott - Q01-es �s Q02-es sorok sz�ma:');
    --dbms_output.put_line('M�r t�bl�ban van kor�bbr�l Q01-es: ' || to_char(sorok));
    
	for i in (select filename, count(*)  db from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end group by filename order by filename)
	loop
	    dbms_output.put_line(i.filename||':'||to_char(i.db));
	end loop;
	eddig:=8;
    begin
        select nvl(max(sorszam),0) into sorok1 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q02mellekletnev=filename;
    exception
        when no_data_found then 
            sorok1:=0;
    end;
	eddig:=9;
    if (sorok > 0 or sorok1 > 0) then
        dbms_output.put_line('A t�bl�ban tal�lhat� - m�r lev�logatott - Q01-es �s Q02-es sorok sz�ma:');
        dbms_output.put_line('M�r t�bl�ban van kor�bbr�l Q01-es: ' || to_char(sorok));
        dbms_output.put_line('M�r t�bl�ban van kor�bbr�l Q02-es: ' || to_char(sorok1));
    end if;
	--dbms_output.put_line('Utols� lev�logat�s: ' || to_char(utolso_futas, 'YYYYMMDD HH24:MI:SS') || '.');
	open gszr_cur(utolso_futas,programneve);
	--dbms_output.put_line('kurzor nyitva');
	fetch gszr_cur into gszr_rec;
	--dbms_output.put_line('fetch k�sz');
    if gszr_cur%notfound then
      /*  sor:='Q01,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
        $if $$debug_on $then       
            insert into vb_rep.mnb_napi_debug
                values ('Q01',q01mellekletnev,1,sor);
        $else              
            insert into vb_rep.mnb_napi
                values ('Q01',q01mellekletnev,1,sor);
        $end  */
        null;
    else 
	--ha szerverhiba miatt nem futott a program, a visszamen� napokra �res �zeneteket kell k�pezni
	
	if sysdate-utolso_futas >2 then
	    dbms_output.put_line('Az utols� lev�logat�s �ta eltelt napok sz�ma: ' || to_char(sysdate-utolso_futas) || '.');
		for nap in 1..floor(sysdate-utolso_futas)
		loop
		    puffersorok:=puffersorok+1;
			/*sor:='Q01,'||to_char(sysdate-nap,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
				begin
				$if $$debug_on $then    
					dbms_output.put_line('�rom a(z) '||to_char(nap)||' nappal ezel�tti �res �zeneteket:  '||sor);
					insert into vb_rep.mnb_napi_debug
						values ('Q01','Q01'||substr(to_char(sysdate-nap,'YYYYMMDD'),4,5)||'15302724',1,sor);
				$else              
					insert into vb_rep.mnb_napi
						values ('Q01','Q01'||substr(to_char(sysdate-nap,'YYYYMMDD'),4,5)||'15302724',1,sor);
				$end  
				exception when others then
					insert into vb.vb_uzenet values (serr,programneve||datum_kar,'Q01 puffer sor insert hiba');
				end;
			sor:='Q02,'||to_char(sysdate-nap,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
				begin
				$if $$debug_on $then       
					insert into vb_rep.mnb_napi_debug
						values ('Q02','Q02'||substr(to_char(sysdate-nap,'YYYYMMDD'),4,5)||'15302724',1,sor);
				$else              
					insert into vb_rep.mnb_napi
						values ('Q02','Q02'||substr(to_char(sysdate-nap,'YYYYMMDD'),4,5)||'15302724',1,sor);
				$end  
			exception when others then
				insert into vb.vb_uzenet values (serr,programneve||datum_kar,'Q02 puffer sor insert hiba');
			end;		*/
		end loop;
		commit;
	end if;
	--Innen pedig j�n az aktu�lis v�ltoz�sok lev�logat�sa
        --levalogatva:=sorok;
        rekordsorszam:=1;
		--dbms_output.put_line(to_char(rekordsorszam));
		eddig:=10;
        loop
            exit when gszr_cur%notfound;
			w_m003:=gszr_rec.m003;
            mnb_rec.Q01:='Q01';                                      -- adatgy�jt�s k�dja
            mnb_rec.datum1:=to_char(datum,'YYYYMMDD');               -- vonatkoz�si id� 8 hosszon
            mnb_rec.KSH_torzsszam:='15302724';                       -- KSH t�rzssz�ma
            mnb_rec.kitoltes_datum:=to_char(datum,'YYYYMMDD');       -- kit�lt�s d�tuma 8 hosszon
            mnb_rec.kshtorzs:='E,KSHTORZS,@KSHTORZS';                -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
            mnb_rec.rekordsorszam:=substr(to_char(rekordsorszam,'0999999'),2,7);             -- sorsz�m 7 karakteren el�null�zva  a kurzor rtekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
 		   
			w_statteaor:=null;
			w_stathataly:=null;
			w_hatalyvege:=null;
			w_stat_r:=null;
			w_kuldes_vege:=null;
			--dbms_output.put_line(to_char(gszr_rec.m003));
                begin
                    select 
					    to_char(gszr_rec.m003),         -- t�rzssz�m, 
                        m0491,
                        to_char(m0491_h,'YYYYMMDD'),    --GFO hat�ly
                        m005_szh,                       --sz�khely megyek�dja, 
                        to_char(m005_szh_h,'YYYYMMDD'), --a megyek�d hat�ly d�tuma
                        case when instr(nev,'''')>0 or instr(nev,'"')>0 or instr(nev,',')>0 then
                          '"'||replace(substr(nev,1,nev_hossz-(REGEXP_COUNT(nev, '"')+2)),'"','""')||'"'
		                else
		                   substr(nev,1,nev_hossz)
		                end,                           -- n�v
   					    to_char(nev_h,'YYYYMMDD'),     --n�v hat�lya
                        case when instr(rnev,'''')>0 or instr(rnev,'"')>0 or instr(rnev,',')>0 then
                          '"'||replace(substr(rnev,1,rnev_hossz-(REGEXP_COUNT(rnev, '"')+2)),'"','""')||'"'
		                else
		                   substr(rnev,1,rnev_hossz)
		                end,                         --r�vid n�v
                        to_char(rnev_h,'YYYYMMDD'),  --r�vid n�v hat�lya
                        rtrim(to_char(m054_szh)),    --sz�khely ir�ny�t� sz�m
                        case when instr(telnev_szh,'''')>0 or instr(telnev_szh,'"')>0 or instr(telnev_szh,',')>0 then
                          '"'||replace(substr(telnev_szh,1,telnev_hossz-(REGEXP_COUNT(telnev_szh, '"')+2)),'"','""')||'"'
		                else
		                   substr(telnev_szh,1,telnev_hossz)
		                end,                                   --sz�khely
                        case when instr(utca_szh,'''')>0 or instr(utca_szh,'"')>0 or instr(utca_szh,',')>0 then
                          '"'||replace(substr(utca_szh,1,utca_hossz-(REGEXP_COUNT(utca_szh, '"')+2)),'"','""')||'"'
		                else
		                   substr(utca_szh,1,utca_hossz)
		                end,                                    --sz�khely utca, h�zsz�m
                        to_char(szekhely_h,'YYYYMMDD'),         --sz�khely c�m hat�lya   		
                        rtrim(to_char(M054_LEV)),               --levelez�si c�m ir�ny�t� sz�ma
                        case when instr(telnev_lev,'''')>0 or instr(telnev_lev,'"')>0 or instr(telnev_lev,',')>0 then
                          '"'||replace(substr(telnev_lev,1,telnev_hossz-(REGEXP_COUNT(telnev_lev, '"')*2+2)),'"','""')||'"'
		                else
		                   substr(telnev_lev,1,telnev_hossz)
		                end,                                     --levelez�si c�m telep�l�s n�v
                        case when instr(utca_lev,'''')>0 or instr(utca_lev,'"')>0 or instr(utca_lev,',')>0 then
                          '"'||replace(substr(utca_lev,1,utca_hossz-(REGEXP_COUNT(utca_lev, '"')+2)),'"','""')||'"'
		                else
		                   substr(utca_lev,1,utca_hossz)
		                end,                                      --levelez�si c�m utca
                        decode(levelezesi_r,null,'',to_char(levelezesi_r,'YYYYMMDD')),  --levelez�si c�m hat�lya (rendszerbe ker�l�se)
                        to_char(m054_pf_lev),                     --postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                        case when instr(telnev_pf_lev,'''')>0 or instr(telnev_pf_lev,'"')>0 or instr(telnev_pf_lev,',')>0 then
                          '"'||replace(substr(telnev_pf_lev,1,telnev_hossz-(REGEXP_COUNT(telnev_pf_lev, '"')+2)),'"','""')||'"'
		                else
		                   substr(telnev_pf_lev,1,telnev_hossz)
		                end,                                        -- postafi�kos levelez�si c�m telep�l�s neve
                        case when instr(pfiok_lev,'''')>0 or instr(pfiok_lev,'"')>0 or instr(pfiok_lev,',')>0 then
                          '"'||replace(substr(pfiok_lev,1,pfiok_lev_hossz-(REGEXP_COUNT(pfiok_lev, '"')+2)),'"','""')||'"'
		                else
		                   substr(pfiok_lev,1,pfiok_lev_hossz)
		                end,                                           -- postafi�k			
                        decode(lev_pf_r,null,'',to_char(lev_pf_r,'YYYYMMDD')), --pf. c�m hat�lya (rendszerbe ker�l�se)
                        m040,                                                  --m�k�d�s �llapotk�dja 
						to_char(m040k,'YYYYMMDD'),                                                         --m�k�d�si k�d hat�lya
                        decode(mukodv,null,'',to_char(mukodv,'YYYYMMDD')),             --m�k�d�s v�ge
                        m025,                                   -- l�tsz�m kateg�ria
                        decode(letszam_h,null,'',to_char(letszam_h,'YYYYMMDD')), --l�tsz�m kateg�ria besorol�s d�tuma			
                        m026,            --�rbev�tel kateg�ria	
						decode(arbev_h,null,to_char(alakdat,'YYYYMMDD'),to_char(arbev_h,'YYYYMMDD')),   --�rbev�tel kateg�ria hat�lya 
						m009_szh,
						decode(alakdat,null,'',to_char(alakdat,'YYYYMMDD')),            --alakul�s d�tuma
                        m0781,                                                          --admin szak�g 2008
                        to_char(m0781_h,'YYYYMMDD'),   --2008-as besorol�si k�d hat�lya
                        m058_j,                        --janus TE�OR
                        to_char(m0581_h,'YYYYMMDD'),   --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                        decode(MP65, 'S9900', null, MP65),   --decode(m063,null,'90',m063)                            --ESA szektork�d
                        to_char(MP65_H, 'YYYYMMDD'),  --to_char(decode(m063_h,null,alakdat,m063_h),'YYYYMMDD')  --ESA szektork�d hat�lya
                        decode(ueleszt,null,'',to_char(ueleszt,'YYYYMMDD')),  --�jra�leszt�s hat�lya
                        decode(m003_r,null,'',to_char(m003_r,'YYYYMMDD')),    --rendszerbe ker�l�s d�tuma
                        to_char(datum,'YYYYMMDD'),     --lev�logat�s d�tuma
                        m0581,                         --statisztikai TE�OR
                        to_char(m0581_h,'YYYYMMDD'),   --stathat�ly
					    cegv,                           -- c�gjegyz�k sz�m
						to_char(cegv_h,'YYYYMMDD'),     -- hat�lya
						nvl(mvb39,'0'),                 -- IFRS nyilatkozat ha null, akkor legyen 0
						nvl(to_char(mvb39_h,'YYYYMMDD'),case when to_char(alakdat,'YYYY')<'2016' then '20160101' else to_char(alakdat,'YYYYMMDD') end),    -- hat�lya ha nincsen akkor az alakul�s d�tuma kiv�ve, ha az alakul�s 2016.01.01-n�l r�gebbi, akkor 2016.01.01
						null,
                        nvl(to_char(LETSZAM), 'N/A'),
                        nvl(to_char(ARBEV), 'N/A'),
                        -- rendszerbe ker�l�sek a vizsg�latokhoz:	
						m003_r,                         -- t�rzssz�m rendszerbe ker�l�se
                        m0491_r, --'YYYYMMDD'),
                        m005_szh_r, --'YYYYMMDD'),
                        nev_r, --'YYYYMMDD'),
                        rnev_r, --'YYYYMMDD'),
                        szekhely_r, --'YYYYMMDD'),
                        m040k_r, --'YYYYMMDD'),
                        m040v_r, --'YYYYMMDD'),
                        m0781_r, --'YYYYMMDD'),
                        m0581_r, --'YYYYMMDD'),
                        MP65_r, --'YYYYMMDD'),  --m063_r
                        arbev_r, --'YYYYMMDD'),
						levelezesi_r,
                        lev_pf_r, 
                        letszam_R,--Szil�gyi �d�m 2022.10.19 _R _h helyett
						cegv_r,
						mvb39_r,
                        UELESZT_R--Szil�gyi �d�m 2022.10.19.
                    into 
					    mnb_rec.torzsszam,            -- 8
                        mnb_rec.gfo,                  -- 3
                        mnb_rec.gfo_hataly,           --GFO hat�ly  8
                        mnb_rec.megyekod,             --sz�khely megyek�dja,  2
                        mnb_rec.megyekod_hataly,      --a megyek�d hat�ly d�tuma  8
                        mnb_rec.nev,                  --n�v  250
                        mnb_rec.nev_h,                --n�v hat�lya  8
                        mnb_rec.rnev,                 --r�vid n�v  40
                        mnb_rec.RNEV_H,               --r�vid n�v hat�lya  8
                        mnb_rec.M054_SZH,             --sz�khely ir�ny�t� sz�m  4
                        mnb_rec.telnev_szh,           --sz�khely	20
                        mnb_rec.utca_szh,             --sz�khely utca, h�zsz�m  80
                        mnb_rec.SZEKHELY_H,           --sz�khely c�m hat�lya 	8
                        mnb_rec.M054_LEV,	          --levelez�si c�m ir�ny�t� sz�ma  4
                        mnb_rec.telnev_lev,           --levelez�si c�m telep�l�s n�v	20
                        mnb_rec.utca_lev,             --levelez�si c�m utca	   80
                        mnb_rec.LEVELEZESI_R,         --levelez�si c�m rendszerbe ker�l�se	8
                        mnb_rec.m054_pf_lev,          --postafi�kos levelez�si c�m ir�ny�t� sz�ma   8
                        mnb_rec.telnev_pf_lev,        --postafi�kos levelez�si c�m telep�l�s neve   4
                        mnb_rec.pfiok_lev,            --postafi�k    20
                        mnb_rec.leV_PF_R,             --pf. c�m hat�lya (rendszerbe ker�l�se)    8
                        mnb_rec.M040,                 --m�k�d�s �llapotk�dja      
                        mnb_rec.m040k,                --m�k�d�si �llapot hat�lya	8					
                        mnb_rec.mukodv,               --m�k�d�s v�ge                8
                        mnb_rec.m025,                 --l�tsz�m kateg�ria           2
                        mnb_rec.letszam_h,            --l�tsz�m kateg�ria besorol�s d�tuma  8
                        mnb_rec.m026,                 --�rbev�tel kateg�ria    1
						mnb_rec.arbev_h,              --�rbev�tel kateg�ria hat�lya    8
						mnb_rec.m009_szh,             --sz�khely telep�l�s k�dja        5
                        mnb_rec.alakdat,              --alakul�s d�tuma                8
                        mnb_rec.m0781,                --admin szak�g 2008                 4
                        mnb_rec.m0781_h,              --2008-as besorol�si k�d hat�lya    8
                        mnb_rec.m058_j,               --janus TE�OR                        4
                        mnb_rec.m0581_j_h,            --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)   8
                        mnb_rec.MP65,        --m063        --ESA szektork�d                       2
                        mnb_rec.MP65_H,      --m063_h         --ESA szektork�d hat�lya                8
                        mnb_rec.ueleszt,              --�jra�leszt�s hat�lya                  8
                        mnb_rec.m003_r,               --szervezet rendszerbe ker�l�se         8
                        mnb_rec.datum2,               --napi lev�logat�s d�tuma                8
                        mnb_rec.statteaor,            --statisztikai TE�OR                     4
                        mnb_rec.stathataly,           --statisztikai TE�OR hat�lya             8
						mnb_rec.cegjegyz,             --c�gjegyz�k sz�m
						mnb_rec.cegjegyz_h,           --c�gjegyz�ksz�m hat�lya
						mnb_rec.mvb39,                --IFRS nyilatkozat
						mnb_rec.mvb39_h,          	  --IFRS hat�lya
                        mnb_rec.ORSZ,                 --Orsz�gk�d
                        mnb_rec.LETSZAM,              --L�tsz�m
                        mnb_rec.ARBEV,                --�rbev�tel
						-- rendszerbe ker�l�sek a vizsg�latokhoz d�tum t�pus�ak:
                        mnb_rec.m003_r_date,
                        mnb_rec.m0491_r,
                        mnb_rec.m005_szh_r,
                        mnb_rec.nev_r,
                        mnb_rec.rnev_r,
						mnb_rec.szekhely_r,
                        mnb_rec.m040k_r,
                        mnb_rec.m040v_r,
                        mnb_rec.m0781_r,
                        mnb_rec.m0581_r,
                        mnb_rec.MP65_R,  --mnb_rec.m063_r
                        mnb_rec.arbev_r,
						mnb_rec.levelezesi_r_date,
                        mnb_rec.lev_pf_r_date,	
                        mnb_rec.letszam_h_date,
						mnb_rec.cegjegyz_r,
						mnb_rec.mvb39_r,
                        mnb_rec.UELESZT_R--Szil�gyi �d�m 2022.10.19.
                    from 
                        vb.f003 
                    where m003=gszr_rec.m003;
                exception
                    when others then 
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||to_char(gszr_rec.m003)||':'||datum_kar,' F003 select hiba');
                        commit;
						--tesztel�shez!
						dbms_output.put_line('HIBA a ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�mn�l, amelynek �zenete: ' || sqlerrm || '.');
						--dbms_output.put_line(to_char(length(mnb_rec.nev))||':250');
                        exit;
						--tesztel�shez!
     			end;
				--dbms_output.put_line( mnb_rec.torzsszam);
				--telep�l�s k�d + cdv
                begin
                    select g.m009_szh||f.m009cdv into mnb_rec.m009_szh from f009_akt f,vb.f003 g where g.m003=gszr_rec.m003 and f.m009=g.m009_szh;
                exception
                    when no_data_found then                   --azon elk�pzelhetetl�l (?) ritka esetekben, ha az avar kori telep�l�st nem tal�ln�nk meg az F009-ben:
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam)||' '||to_char(mnb_rec.m009_szh),' nincs cdv a telep�l�shez'||datum_kar);
                        commit;
                        mnb_rec.m009_szh:=mnb_rec.m009_szh||'X';
                end;                               --sz�khely telep�l�s k�d+cdv 5 jegyen	
                eddig:=11;				
                --dbms_output.put_line('cdv k�sz');   	
                
                begin
                    SELECT ORSZ into mnb_rec.ORSZ
                      FROM (SELECT distinct M003, ORSZ,
                                   DATUM_R,
                                   rank() over (partition by M003 order by DATUM_R desc) rnk
                              FROM VB_CEG.VB_APEH_CIM where M003 = gszr_rec.m003)
                     WHERE rnk = 1;
                     
                     
                     if(mnb_rec.ORSZ = 'HU' and mnb_rec.megyekod > 20) then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja HU-r�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                     if(mnb_rec.ORSZ = 'XX') then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja XX-r�l Z8 lett, mert XX �rt�ket az MNB nem tud fogadni.');
                     end if;
                     
                     --Amennyiben �res orsz�gk�ddal van bent egy c�g a VB_CEG.VB_APEH_CIM adatb�zis t�bl�ban
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                        mnb_rec.ORSZ := 'HU'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l HU lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                     end if;
                     
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                exception
                    when no_data_found then
                    
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                            mnb_rec.ORSZ := 'HU'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l HU lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                         end if;
                     
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                            mnb_rec.ORSZ := 'Z8'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                         end if;
                         
                end;
                eddig:=110;
                
                
                begin
                    if(mnb_rec.MP65 is null ) then 
                        mnb_rec.MP65_H := null; 
                        --dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m MP65_H �rt�ke �res lett.');
                     end if;
                end;
                
                begin
                    if (ALAKDAT_MODSZAM > 0) then
                        select M003 into M003_ALAKDAT_CHANGED from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 = gszr_rec.m003;  
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m alakul�s d�tuma megv�ltozott a k�vetkez�re: ' || to_char(mnb_rec.alakdat) || '.');
                    end if;     
                    
                    exception
                        when no_data_found then
                        null;
                             
                end;
                
-- A GSZR-rekord kigy�jt�s�nek v�ge.
-- Van-e v�ltoz�s a GSZR-rekordban vagy az F003_m0582-ben?		
                nsz_db:=0;
                begin
                        select count(*) into nsz_db from vb.f003_m0582 g
                        where g.m003=gszr_rec.m003 and g.m0582_r=(select max(m0582_r) from 
						vb.f003_m0582 where m003=gszr_rec.m003);
                exception
                    when others then
                        serr:=sqlcode; 
                        errmsg:=substr(sqlerrm,1,40);
                        insert into  vb.vb_uzenet (number1,text1,text2) 
                              values(serr,'mnb napi adatk�ld�:F003_m0582 lek�rdez�s'||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
                        commit; 
                end;
				eddig:=12;
                if nsz_db=1 then
                 -- csak egy nsz-rekord van utols�
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003=gszr_rec.m003 and m0582_r=(select max(m0582_r) from  vb.f003_m0582 
					                                      where m003=gszr_rec.m003);
                elsif nsz_db>1 then
                 --egy nap t�bb v�ltoz�s volt. Ezek k�z�l csak egy lehet nyitott: nem lez�rt, az kell
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege  into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003=gszr_rec.m003 and m0582_r=(select max(m0582_r) from vb.f003_m0582
					                                      where m003=gszr_rec.m003)
                    and m0582_hv is null;
                end if;
			    eddig:=13;
--a hat�lyokat kalkul�lgatjuk:			  
                if nsz_db!=0 then
                    --dbms_output.put_line('nsz_rekord:'||to_char(nsz_db));
    				--dbms_output.put_line('m0582:'||w_statteaor);
				    --dbms_output.put_line('hat�ly:'||to_char(w_stathataly,'YYYYMMDD'));
                    --dbms_output.put_line('hat�ly v�ge:'||to_char(w_hatalyvege,'YYYYMMDD'));
		    	    --dbms_output.put_line('rendszerbe ker�l�se:'||to_char(w_stat_r,'YYYYMMDD'));
	    		    --dbms_output.put_line('k�ld�s  v�ge:'||to_char(w_kuldes_vege,'YYYYMMDD'));
    				if w_hatalyvege is not null then
					    eddig:=14;
               --most z�rtuk le vagy k�l�n list�n k�rt�k
                        --dbms_output.put_line('nsz!=0');
               -- a statteaor marad a GSZR-b�l leszedett? Vagy az utols� lez�r�s hat�lya?
                        mnb_rec.stathataly:=greatest(to_char(w_hatalyvege,'YYYYMMDD'),mnb_rec.stathataly);
                     --Most z�rtuk le, k�ldeni kell, ez�rt le kell v�logatni a Nemzeti sz�ml�s k�rbe ker�l�st�l kezd�d�en visszamen�leg is a statisztikai TE�OR id�sor�t
                        --dbms_output.put_line('T�rzssz�m:');
                        --dbms_output.put_line(to_char(mnb_rec.torzsszam)||'; m0582_hv:'||mnb_rec.stathataly);
                        begin
						    --dbms_output.put_line('max m0582_r');
                            select max(m0581_h) into maxm0581r 
                            from vb.f003_hist4 where m003=mnb_rec.torzsszam and m0581_h>=to_date(mnb_rec.stathataly,'YYYYMMDD');
                        exception
                            when no_data_found then
                                null;
                            when too_many_rows then
                                select m  into maxm0581r from(select rownum r,max(m0581_h) m from vb.f003_hist4 
                                where m003=mnb_rec.torzsszam and m0581_h=maxm0581r) where r=1;
                        end;  
						eddig:=15;
                        --dbms_output.put_line('Hist4-b�l max d�tum:'||nvl(to_char(maxm0581r,'YYYYMMDD'),'lez�r�s �ta nincsen hist�ria'));
                        select count(*) into lezarasdb from vb.f003_hist4 where m003=mnb_rec.torzsszam and 
                                               m0581_h>=maxm0581r and m0581 is not null order by m0581_h, m0581_r;
						eddig:=16;					   
                        --dbms_output.put_line('Mennyi te�or v�ltoz�s volt a lez�r�s �ta:'||to_char(lezarasdb));
  /*                     
  					   if lezarasdb>0 then
                            for rec in (select rownum r,m003,m0581,m0581_h from vb.f003_hist4 where m003=mnb_rec.torzsszam and 
                                m0581_h<=maxm0581r and m0581_h>=to_date(mnb_rec.stathataly,'YYYYMMDD') and m0581 is not null order by m0581_h, m0581_r)
                            loop
                                if rec.r=1 then 
                                    dbms_output.put_line('Stat TE�OR id�sora:');
                                    dbms_output.put_line('m003    :Stat:Hat�lya ');     
                                end if;
                                dbms_output.put_line(to_char(rec.m003)||':'||rec.m0581||':'||to_char(rec.m0581_h,'YYYYMMDD'));     
                            end loop;
                        else
                            dbms_output.put_line(to_char(mnb_rec.torzsszam)||':'||'nsz lez�rva:'||mnb_rec.stathataly||' nincs historia');                      
                        end if;
						*/
                        lezarasdb:=0;
                    else      --nsz_db!=0 and w_hatalyvege is null then 
                     --most v�ltozott az NSZ-TE�OR!
					    --dbms_output.put_line('hatalyvege is null');
                        mnb_rec.statteaor:=w_statteaor;
                        mnb_rec.stathataly:=to_char(w_stathataly,'YYYYMMDD');
						eddig:=17;
                    end if;	
                end if; --nsz_db!=0
                 --l�tsz�m- �rbev�tel kateg�ria, �s ESA k�d
				--dbms_output.put_line('nsz-TE�OR k�sz');
                w_m025:='  ';
                w_m026:=' ';
              --  w_m063:='  ';
               --legy�jtj�k az ESA k�dot, a l�tsz�m kateg�ri�t,�s �rbev�tel kateg�ri�t a histb�l (ha benne van)
               /* begin
                    select m063 into w_m063 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                         null;
                end; */
                eddig:=18;				
                --dbms_output.put_line('ESA k�d');            
                begin
                    select m025 into w_m025 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                       null;
                end; 
                eddig:=19;				
                --dbms_output.put_line('L�tsz�m kateg�ria');                  
                begin
                    select m026 into w_m026 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                        null;
                end;
                eddig:=20;				
                mehet:=false;
--megn�zz�k, mi�rt ker�lt a kurzorba a t�rzssz�m				
                begin
                    select count(*) into kulondb from vb_rep.vb_app_init where 
					param_nev='m003' 
					and  program=programneve
					and m003=gszr_rec.m003  and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas;
                exception
                    when others then
					    serr:=sqlcode;
                        kulondb:=0;
						--dbms_output.put_line('k�l�ndb exception :'||to_char(serr));
                end;
				--dbms_output.put_line(to_char(gszr_rec.m003)||':k�l�ndb:'||to_char(kulondb));
				eddig:=21;
				--dbms_output.put_line('van-e kulondb:'||to_char(kulondb));
                if kulondb!=0 or 
				   ((w_stathataly >= utolso_futas and w_statteaor!=mnb_rec.statteaor and w_kuldes_vege is null) or --a nemzeti sz�ml�s TE�OR az utols� fut�s �ta ker�lt be
                   (w_kuldes_vege>= utolso_futas)) then
                      mehet:=true;
					  EDDIG:=22;
                  --dbms_output.put_line('mehet1');                         
                else   
                  --dbms_output.put_line('mehet2');                    
                 -- nem �rbev�tel, l�tsz�m kat., vagy ESA k�d miatt ker�lt a kurzorba
				    eddig:=23;
                    begin      
                      --�s statisztika gy�jt�se					
						if mnb_rec.m003_r_date  >= utolso_futas then 
							m003_r_db:=m003_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m003_r');     
						if mnb_rec.m0491_r      >= utolso_futas then 
							m0491_r_db:=m0491_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m0491_r'); 
						if mnb_rec.m005_szh_r   >= utolso_futas then 
							m005_szh_db:=m005_szh_db+1;
							mehet:=true; end if;
						--dbms_output.put_line('m005_r'); 
						if mnb_rec.nev_r        >= utolso_futas then 
							nev_r_db:=nev_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('nev_r'); 
						if mnb_rec.rnev_r       >= utolso_futas then 
							rnev_r_db:=rnev_r_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_rnev := m003_r_db_for_rnev + 1; 
                            end if;
						end if;
						--dbms_output.put_line('rnev_r'); 
						if mnb_rec.szekhely_r   >= utolso_futas then 
							szekhely_r_db:=szekhely_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('szekhely_r'); 
						if mnb_rec.levelezesi_r_date >= utolso_futas then 
							levelezesi_r_db:=levelezesi_r_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_levelezesi := m003_r_db_for_levelezesi + 1; 
                            end if;
						end if;
						--dbms_output.put_line('levelezesi_r'); 
						if mnb_rec.LEV_PF_R_date     >= utolso_futas then 
							LEV_PF_R_db:=LEV_PF_R_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_lev_pf := m003_r_db_for_lev_pf + 1; 
                            end if;
						end if;
						--dbms_output.put_line('lev_pf_r'); 
						if mnb_rec.m040k_r      >= utolso_futas then 
							m040k_r_db:=m040k_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m040k_r');
						if mnb_rec.m040v_r      >= utolso_futas then 
							m040v_r_db:=m040v_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m040v_r'); 
						if mnb_rec.m0781_r      >= utolso_futas then 
							m0781_r_db:=m0781_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m0781_r'); 
						if mnb_rec.letszam_h_date>= utolso_futas then
							letszam_h_db:=letszam_h_db+1;
						end if;	
						--dbms_output.put_line('letszam'); 
						if mnb_rec.arbev_r>=utolso_futas then
							arbev_r_db:=arbev_r_db+1;
						end if;
						--dbms_output.put_line('ARBEV');
						if mnb_rec.m0581_r>=utolso_futas then
							m0581_r_db:=m0581_r_db+1;
						end if;
						--dbms_output.put_line('StatTE�OR');
						if mnb_rec.MP65_r >= utolso_futas then  --m063_r
							MP65_r_db := MP65_r_db + 1; --m063_r_db:=m063_r_db+1;
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_MP65 := m003_r_db_for_MP65 + 1;  --m003_r_db_for_m063 := m003_r_db_for_m063 + 1; 
                            end if;
						end if;	
						--dbms_output.put_line('ESA');
						if mnb_rec.ueleszt_R>=utolso_futas then--Szil�gyi �d�m 2022.10.19. _R
							ueleszt_db:=ueleszt_db+1;
						end if;
						--dbms_output.put_line('c�gjegyz�k sz�m);
						if mnb_rec.cegjegyz_r>=utolso_futas then
							cegv_db:=cegv_db+1;
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_cegv := m003_r_db_for_cegv + 1; 
                            end if;
						end if;
						--dbms_output.put_line('IFRS');
						if mnb_rec.mvb39_r>=utolso_futas then
							ifrs_db:=ifrs_db+1; 
						end if;
						--dbms_output.put_line('ueleszt');
	/*                  if mnb_rec.m0581_r    >= to_char(utolso_futas,'YYYYMMDD') then mehet:=true; end if;
						dbms_output.put_line('m0581_r'); */
						--dbms_output.put_line(mnb_rec.ueleszt);
					exception when others then
					   dbms_output.put_line(to_char(sqlcode));
					end;
					eddig:=24;
					if nvl(TO_DATE(mnb_rec.ueleszt,'yyyymmdd'),sysdate)  >= utolso_futas then mehet:=true; end if;
                    --dbms_output.put_line('ueleszt'); 
					eddig:=25;
                    megszunt:=false; 
                    if (mnb_rec.m040 not in ('0','9') or substr(mnb_rec.m040k,1,4)=to_char(sysdate,'YYYY'))  -- �l�, vagy az adott napt�ri �vben sz�nt meg --substr(mnb_rec.m040k,1,1) volt substr(mnb_rec.m040k,1,4) helyett, de �gy csak az �vsz�m els� jegye volt hasonl�tva YYYY-al
                              or 
                            ( mnb_rec.m040 in ('0','9') and mnb_rec.m040k_r>=utolso_futas) or kulondb>0  then 
                            mehet:=true;
                            --dbms_output.put_line(mnb_rec.torzsszam||' mehet');	
	                else 
                             mehet:=false;
                              megszunt:=true;
                              --dbms_output.put_line(mnb_rec.torzsszam||' megpusztult,'||mnb_rec.m040k||' nem mehet');	
							 dbms_output.put_line('A k�vetkez� t�rzssz�m kihagyva, mert kor�bbi �vben (' || substr(mnb_rec.m040k, 1, 4) || '-' || substr(mnb_rec.m040k, 5, 2) || '-' ||  substr(mnb_rec.m040k, 7, 2) || ') sz�nt meg: ' || mnb_rec.torzsszam || '.');	
                    end if;--vagy, ha a megsz�n�si inform�ci� csak most ker�lt a regiszterbe.
					eddig:=26;
                  --dbms_output.put_line('nem �rbev�tel, l�tsz�m kat., vagy ESA k�d miatt ker�lt a kurzorba');   
                 --kiz�r� okok
                 --m�s ok nincs csak �rbev,esa vagy l�tsz�m kat v�ltoz�s, csak akkor mehet, ha effekt�v v�ltoz�s volt
                    --dbms_output.put_line('ESA vagy �rbev vagy l�tsz�m v�ltozott-e');
                    --if(mehet = false) then
                      --  dbms_output.put_line(mnb_rec.torzsszam || ': ' || sys.diutil.bool_to_int(mehet) || ', ' || sys.diutil.bool_to_int(megszunt) || ', ' || sys.diutil.bool_to_int(not mehet and not megszunt));
                    --end if;
                    if not mehet and not megszunt then
                       /* if mnb_rec.m063_r       >= utolso_futas and mnb_rec.m063!=w_m063 then
                             mehet:=true;
                             --dbms_output.put_line('ESA_r miatt mehet');							 
                        elsif mnb_rec.m063=w_m063 then
                            dbms_output.put_line('Nem v�ltozott:'||to_char(mnb_rec.torzsszam)||' m063:'||w_m063||'='||mnb_rec.m063);
                            nem_valtozott:=nem_valtozott+1;
                        end if;*/
						eddig:=27;
                        if mnb_rec.letszam_h_date    >= utolso_futas and mnb_rec.m025!=nvl(w_m025,'00') then 
                            mehet:=true; 
							--dbms_output.put_line('letszam_h miatt mehet');							 
                        elsif mnb_rec.m025=nvl(w_m025,'00') then     
                            dbms_output.put_line('Nem v�ltozott:'||to_char(mnb_rec.torzsszam)||' m025:'||w_m025||'='||mnb_rec.m025);                       
                            nem_valtozott:=nem_valtozott+1;
                        end if;  
                        eddig:=28;						
                        if mnb_rec.arbev_r      >= utolso_futas and mnb_rec.m026!=nvl(w_m026,'0') then
                            mehet:=true; 
							--dbms_output.put_line('arbev_r miatt mehet');	
                        elsif mnb_rec.m026=nvl(w_m026,'0') then                       
                            dbms_output.put_line('Nem v�ltozott:'||to_char(mnb_rec.torzsszam)||' m026:'||w_m026||'='||mnb_rec.m026);  
                            nem_valtozott:=nem_valtozott+1;                                   
                        end if; 
						eddig:=29;
						--2019.09.06.  Levizsg�ljuk, hogy megv�ltozott-e az alakul�s d�tuma ut�lag, hogy ha m�s nem v�ltozott, akkor is benne maradjon a 
                        --           lev�logat�sban
						w_alakdat:=null;
						begin
							select alakdat_u into w_alakdat from vb.f003_hist3pr where
							m003=mnb_rec.ksh_torzsszam and datum>utolso_futas and alakdat!=alakdat_u;
						exception when no_data_found then
						     null;
						end;
						if w_alakdat is not null then 
						--megv�ltozott az alakul�s d�tuma, mindenhogyan �t kell adni!
						    mehet:=true;
						end if;	
                    end if;
                end if;
                --dbms_output.put_line('m026 a hist2-b�l');                   
                --dbms_output.put_line('Hat�rellen�rz�s j�n');                        
                 ---  K�ldhet� lenne, de valamelyik d�tum hib�s, ez�rt m�gsem k�ldhet�
                if  not to_date(mnb_rec.gfo_hataly,'YYYYMMDD')    between alsohatar and felsohatar  then 
                      mehet:=false; 
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0491_h:'||mnb_rec.gfo_hataly); 
                end if;
				eddig:=30;
                 --dbms_output.put_line('m0491_h'); 
                if not to_date(mnb_rec.megyekod_hataly,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m005_szh_h:'||mnb_rec.megyekod_hataly); 
                end if;
				eddig:=31;
                 --dbms_output.put_line('m005_szh_h'); 
                if not to_date(mnb_rec.nev_h,'YYYYMMDD')        between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                 
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':nev_h:'||mnb_rec.nev_h ); 
                 end if;
				 eddig:=32;
                 begin
				 --dbms_output.put_line('nev_h'); 
                if not nvl(to_date(mnb_rec.RNEV_H,'YYYYMMDD') ,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                   
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':rnev_h:'||mnb_rec.RNEV_H); 
                 end if;
				 exception when others then
				     dbms_output.put_line(to_char( mnb_rec.torzsszam)||':nev_h:'); 
				 end;
				 eddig:=33;
                 --dbms_output.put_line('rnev_h'); 
                if not to_date(mnb_rec.SZEKHELY_H,'YYYYMMDD')    between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':SZEKHELY_H:'||mnb_rec.SZEKHELY_H); 
                 end if;
				 eddig:=34;
                 --dbms_output.put_line('szekhely_h'); 
                if not nvl(mnb_rec.LEVELEZESI_R_date,sysdate)  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':LEVELEZESI_R:'||mnb_rec.LEVELEZESI_R); 
                 end if;
				 eddig:=35;
                 --dbms_output.put_line('levelezesi_r'); 
                if not nvl(mnb_rec.leV_PF_R_date,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':leV_PF_R :'||mnb_rec.leV_PF_R); 
                 end if;
				 eddig:=36;
                 --dbms_output.put_line('lev_pf_r'); 
                if not nvl(to_date(mnb_rec.mukodv,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':mukodv:'||mnb_rec.mukodv); 
                 end if;
				 eddig:=37;
                 --dbms_output.put_line('mukodv'); 
                if not nvl(to_date(mnb_rec.letszam_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':letszam_h:'||mnb_rec.letszam_h); 
                 end if;
				 eddig:=38;
                 --dbms_output.put_line('letszam_h'); 
                if not nvl(to_date(mnb_rec.arbev_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':arbev_h:'||mnb_rec.arbev_h); 
                 end if;
				 eddig:=39;
                 --dbms_output.put_line('arbev_h'); 
                if not to_date(mnb_rec.alakdat,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':alakdat:'||mnb_rec.alakdat); 
				elsif mnb_rec.torzsszam in ('15302724','15736527')  then 
				       mnb_rec.alakdat:='19830101';
                end if;
				 eddig:=40;
                 --dbms_output.put_line('alakdat'); 
                if not to_date(mnb_rec.m0781_h,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0781_h:'||mnb_rec.m0781_h); 
                 end if;
				 eddig:=41;
                --dbms_output.put_line('m0781_h'); 
				--dbms_output.put_line('stathat�ly:'||mnb_rec.stathataly);
                if not mnb_rec.stathataly  between to_char(alsohatar,'YYYYMMDD') and to_char(felsohatar,'YYYYMMDD') then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0581_h:'||mnb_rec.stathataly); 
                 end if;
				 eddig:=42;
                 --dbms_output.put_line('m0581_h'); 
                if not nvl(to_date(mnb_rec.MP65_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then   --m063_h
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum: '||to_char(mnb_rec.torzsszam) || ' :mp65_h: ' || mnb_rec.MP65_h);  --mnb_rec.m063_h
                 end if;
				 eddig:=43;
                 --dbms_output.put_line('m063_h'); 
                if not nvl(to_date(mnb_rec.ueleszt,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum: '||to_char(mnb_rec.torzsszam)||'ueleszt: '||mnb_rec.ueleszt); 
                 end if;
				 eddig:=44;
                 --dbms_output.put_line('ueleszt'); 
                if not to_date(mnb_rec.m003_r,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||'m003_r :'||mnb_rec.m003_r); 
                 end if;
				 eddig:=45;
                 --dbms_output.put_line('m003_r'); 
                 --dbms_output.put_line('Hat�rellen�rz�s v�ge');      
           -- $if $$debug_on $then
			  -- dbms_output.put_line(mnb_rec.torzsszam||' '||case when mehet then 'mehet' else 'nem mehet' end);
			--$end
            --dbms_output.put_line('D�tumhib�k ellen�rizve');                  
            if mehet then
                sor:=mnb_rec.q01||','||                           -- adatgy�jt�s k�dja
                    mnb_rec.datum1||','||                         -- vonatkoz�si id� 8 hosszon
                    mnb_rec.ksh_torzsszam||','||                  -- KSH t�rzssz�ma
                    mnb_rec.kitoltes_datum||','||                 -- kit�lt�s d�tuma 8 hosszon
                    mnb_rec.kshtorzs||                            -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
                    mnb_rec.rekordsorszam||','||                  -- sorsz�m 7 karakteren el�null�zva  a kurzor rekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
                    mnb_rec.torzsszam||','||                      -- t�rzsszam
                    mnb_rec.gfo||','||                            -- GFO
                    mnb_rec.gfo_hataly||','||                     -- GFO hat�ly
                    mnb_rec.megyekod||','||                       --sz�khely megyek�dja, 
                    mnb_rec.megyekod_hataly||','||                --a megyek�d hat�ly d�tuma
                    mnb_rec.nev||','||                            --n�v
                    mnb_rec.nev_h||','||                          --n�v hat�lya
                    mnb_rec.rnev||','||                           --r�vid n�v
                    mnb_rec.RNEV_H||','||                         --r�vid n�v hat�lya
                    mnb_rec.M054_SZH||','||                       --sz�khely ir�ny�t� sz�m
                    mnb_rec.telnev_szh||','||                     --sz�khely	
                    mnb_rec.utca_szh||','||                       --sz�khely utca, h�zsz�m
                    mnb_rec.SZEKHELY_H||','||                     --sz�khely c�m hat�lya 	
                    mnb_rec.M054_LEV||','||	                      --levelez�si c�m ir�ny�t� sz�ma
                    mnb_rec.telnev_lev||','||                     --levelez�si c�m telep�l�s n�v	
                    mnb_rec.utca_lev||','||             --levelez�si c�m utca	
                    mnb_rec.LEVELEZESI_R||','||         --levelez�si c�m rendszerbe ker�l�se	
                    mnb_rec.m054_pf_lev||','||          --postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                    mnb_rec.telnev_pf_lev||','||        --postafi�kos levelez�si c�m telep�l�s neve
                    mnb_rec.pfiok_lev||','||            --postafi�k
                    mnb_rec.leV_PF_R||','||             --pf. c�m hat�lya (rendszerbe ker�l�se)
                    mnb_rec.M040||','||                 --m�k�d�s �llapotk�dja �tk�dolva: 0,9->0, egy�bk�nt 1               
                    mnb_rec.m040k||','||                --�llapotk�d hat�lya
                    mnb_rec.m025||','||                 --l�tsz�m kateg�ria
                    mnb_rec.letszam_h||','||            --l�tsz�m kateg�ria besorol�s d�tuma
                    mnb_rec.m026||','||                 --�rbev�tel kateg�ria
                    mnb_rec.arbev_h||','||              --�rbev�tel kateg�ria hat�lya
                    mnb_rec.m009_szh||','||             --sz�khely telep�l�s k�d+cdv 5 jegyen					
                    mnb_rec.alakdat||','||              --alakul�s d�tuma
                    mnb_rec.m0781||','||                --admin szak�g 2008
                    mnb_rec.m0781_h||','||              --2008-as besorol�si k�d hat�lya
                    mnb_rec.m058_j||','||               --janus TE�OR
                    mnb_rec.m0581_j_h||','||            --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                    mnb_rec.MP65||','||                 --ESA szektork�d   --mnb_rec.m063
                    mnb_rec.MP65_H||','||               --ESA szektork�d hat�lya   -mnb_rec.m063_h
                    mnb_rec.ueleszt ||','||              --�jra�leszt�s hat�lya
                    mnb_rec.m003_r||','||               --szervezet rendszerbe ker�l�se
                    mnb_rec.datum2||','||               --napi lev�logat�s d�tuma
                    mnb_rec.statteaor||','||            --statisztikai TE�OR
                    mnb_rec.stathataly||','||           --statisztikai TE�OR hat�lya		
                    mnb_rec.cegjegyz||','||             --c�gjegyz�k sz�m
					mnb_rec.cegjegyz_h||','||           --c�gjegyz�ksz�m hat�lya
					mnb_rec.mvb39||','||                --IFRS nyilatkozat
					mnb_rec.mvb39_h||','||              --IFRS nyilatkozat hat�lya
                    mnb_rec.ORSZ||','||                 --Orsz�gk�d
                    mnb_rec.LETSZAM||','||              --L�tsz�m
                    mnb_rec.ARBEV;                      --�rbev�tel 
                    eddig:=46;					
               --t�bl�ba sz�rjuk a k�ldend� rekordot
			   $if $$debug_on $then
                    null;
                   --dbms_output.put_line('insert '||mnb_rec.torzsszam||mnb_rec.stathataly||mnb_rec.rekordsorszam);
               $end
                   begin
                        insert into 
						$if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end (kod,filename,sorszam,rekord)
                        values ('Q01',q01mellekletnev,rekordsorszam,sor);
                        --levalogatva:=levalogatva+1;
                   exception
                        when others then
                            serr:=sqlcode; 
                            errmsg:=substr(sqlerrm,1,40);
                            insert into  vb.vb_uzenet (number1,text1,text2) 
                                                values(serr,'mnb napi adatk�ld�:'||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
                            commit; 
                            kihagyott:=kihagyott+1;                        
                    end;
					eddig:=47;
				rekordsorszam:=rekordsorszam+1;
            else
			    eddig:=48;
                kihagyott:=kihagyott+1;    
            end if;
			eddig:=49;
            --levalogatva:=sorok+mnb_rec.rn-kihagyott;    
            if mod(rekordsorszam+kihagyott,100)=0 then
                   begin
				   $if $$debug_on $then
                       vb.mod_szam_tolt('K865',tablaneve,sorok,'MNB napi v�ltoz�slista k�ld�se, debug m�d ',programneve||verzio,datum,'M');     
                   $else
				       vb.mod_szam_tolt('K865',tablaneve,sorok,'MNB napi v�ltoz�slista k�ld�se',programneve||verzio,datum,'M');
				   $end
                   exception when others then
                       serr:=sqlcode;                   
                       insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','mod_szam_tolt a hiba');
                   end;
                   commit;
            end if;
            eddig:=50;			
			fetch gszr_cur into gszr_rec;
        end loop;
    end if;           
    close gszr_cur;
    
        commit;
        select count(*) into levalogatva_1 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where kod='Q01'
		and substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');-- and substr(rekord, 32, 1) != 'N';--Szil�gyi �d�m 2022.10.21.
		eddig:=51;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('Most lev�logatott Q01-es sorok: ' || levalogatva_1);
       -- else 
       --     dbms_output.put_line('A mai nap folyam�n nem ker�lt Q01-es adat lev�logat�sra.');
        end if;
		if puffersorok>0 then 
		    dbms_output.put_line('Puffer sorok sz�ma:  '||puffersorok);
        end if;		
        if(kihagyott > 0) then 
            dbms_output.put_line('Kihagyott sorok: ' || kihagyott || ', amelyekb�l nem v�ltozott: ' || nem_valtozott || '.');
        end if;
        --dbms_output.put_line('ebb�l: nem v�ltozott:'||nem_valtozott);  
		
		--Statisztik�k:
		dbms_output.put_line('');
    	select count(*) into osszesq01 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where kod='Q01';
		--dbms_output.put_line('A t�bl�ban tal�lhat� Q01-es c�gek sz�ma: ' || to_char(osszesq01) || '.');
        dbms_output.put_line('---');
        --dbms_output.new_line;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('�tfed� statisztik�k, amelyekb�l a m�sodik sz�m az �rintett �j t�rzssz�mok sz�m�val cs�kkentett sz�m: ');
            --dbms_output.put_line('A m�sodik sz�m az �j t�rzssz�mok n�lk�li v�ltoz�sok sz�ma');
            --dbms_output.put_line('');
            dbms_output.new_line;
            if m003_r_db>0 then 
                dbms_output.put_line('�j t�rzssz�m (M003): ' ||to_char(m003_r_db) || '.');
            end if;
            if m0491_r_db>0 then 
                dbms_output.put_line('GFO v�ltoz�s (M0491): ' ||to_char(m0491_r_db)|| ' : ' ||to_char(m0491_r_db-least(m003_r_db,m0491_r_db)) || '.');		
            end if;
            if m005_szh_db>0 then 
                dbms_output.put_line('Megyek�d v�ltoz�s (M005_SZH): '||to_char(m005_szh_db)||' : '||to_char(m005_szh_db-least(m003_r_db,m005_szh_db)) || '.');				
            end if;	
            if nev_r_db>0 then 
                dbms_output.put_line('N�v v�ltoz�s (NEV): '||to_char(nev_r_db)||' : '||to_char(nev_r_db-least(m003_r_db,nev_r_db)) || '.');		
            end if;
            if rnev_r_db>0 and m003_r_db_for_rnev = 0 then 
                dbms_output.put_line('R�vid n�v v�ltoz�s (RNEV): ' || to_char(rnev_r_db) || '.');-- ||' : '||to_char(rnev_r_db-least(m003_r_db,rnev_r_db)) || '.');		
            elsif rnev_r_db>0 and m003_r_db_for_rnev > 0 then
                dbms_output.put_line('R�vid n�v v�ltoz�s (RNEV): ' || to_char(rnev_r_db) || ' : ' || to_char(rnev_r_db - m003_r_db_for_rnev) || '.');		
            end if;
            if szekhely_r_db>0 then 
                dbms_output.put_line('Sz�khely v�ltoz�s (SZEKHELY): '||to_char(szekhely_r_db)||' : '||to_char(szekhely_r_db-least(m003_r_db,szekhely_r_db)) || '.');		
            end if;
            if levelezesi_r_db>0 and m003_r_db_for_levelezesi = 0 then 
                dbms_output.put_line('Levelez�si c�m v�ltoz�s (LEVELEZESI): ' || to_char(levelezesi_r_db) || '.');--||' : '||to_char(levelezesi_r_db-least(m003_r_db,levelezesi_r_db)) || '.');
            elsif levelezesi_r_db>0 and m003_r_db_for_levelezesi > 0 then
                dbms_output.put_line('Levelez�si c�m v�ltoz�s (LEVELEZESI): ' || to_char(levelezesi_r_db) || ' : ' || to_char(levelezesi_r_db - m003_r_db_for_levelezesi) || '.');
            end if;
            if LEV_PF_R_db>0 and m003_r_db_for_lev_pf = 0 then 
                dbms_output.put_line('Postafi�k v�ltoz�s (LEV_PF): ' || to_char(lev_pf_r_db) || '.');-- ||' : '||to_char(lev_pf_r_db-least(m003_r_db,lev_pf_r_db)) || '.');		
            elsif LEV_PF_R_db>0 and m003_r_db_for_lev_pf > 0 then
                dbms_output.put_line('Postafi�k v�ltoz�s (LEV_PF): ' || to_char(lev_pf_r_db) || ' : ' || to_char(lev_pf_r_db - m003_r_db_for_lev_pf) || '.');
            end if;
            if m040k_r_db>0 then 
                dbms_output.put_line('�llapotk�d v�ltoz�s (M040K): '||to_char(m040k_r_db)||' : '||to_char(m040k_r_db-least(m003_r_db,m040k_r_db)) || '.');				
            end if;
            if m040v_r_db>0 then 
                dbms_output.put_line('�llapotk�d v�ge (M040V): ' || to_char(m040v_r_db) || '.');-- || ' : ' || to_char(m040v_r_db-least(m003_r_db,m040v_r_db)) || '.');
            end if;
            if m0781_r_db>0 then 
                dbms_output.put_line('Adminisztrat�v TE�OR v�ltoz�s (M0781): '||to_char(m0781_r_db)||' : '||to_char(m0781_r_db-least(m003_r_db,m0781_r_db)) || '.');
            end if;
            if letszam_h_db>0 then
                dbms_output.put_line('L�tsz�m kateg�ria v�ltoz�s (LETSZAM): '||to_char(letszam_h_db)||' : '||to_char(letszam_h_db-m003_r_db) || '.');
            end if;	
            if arbev_r_db>0 then
                dbms_output.put_line('�rbev�tel-kateg�ria v�ltoz�s (ARBEV): '||to_char(arbev_r_db)||' : '||to_char(arbev_r_db-least(m003_r_db,arbev_r_db)) || '.');		
            end if;
            if m0581_r_db>0 then
                dbms_output.put_line('Statisztikai TE�OR v�ltoz�s (M0581): '||to_char(m0581_r_db)||' : '||to_char(m0581_r_db-least(m003_r_db,m0581_r_db)) || '.');		
            end if;
            /*if m063_r_db>0 and m003_r_db_for_m063 = 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (M063): ' || to_char(m063_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif m063_r_db>0 and m003_r_db_for_m063 > 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (M063): ' || to_char(m063_r_db) || ' : ' || to_char(m063_r_db - m003_r_db_for_m063) || '.');		
            end if;	*/
            if MP65_r_db > 0 and m003_r_db_for_MP65 = 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (MP65): ' || to_char(MP65_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif MP65_r_db > 0 and m003_r_db_for_MP65 > 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (MP65): ' || to_char(MP65_r_db) || ' : ' || to_char(MP65_r_db - m003_r_db_for_MP65) || '.');		
            end if;	
            if ueleszt_db>0 then
                dbms_output.put_line('�jra�leszt�sek (UELESZT): '||to_char(ueleszt_db) || '.');
            end if;
            if ifrs_db>0 then
                dbms_output.put_line('�j IFRS nyilatkozatok (MVB39): '||to_char(ifrs_db) || '.');
            end if;	
            if cegv_db>0 and m003_r_db_for_cegv = 0 then
                dbms_output.put_line('C�gjegyz�ksz�m v�ltoz�sok (CEGV): '||to_char(cegv_db) || '.');
            elsif cegv_db>0 and m003_r_db_for_cegv > 0 then
                dbms_output.put_line('C�gjegyz�ksz�m v�ltoz�sok (CEGV): ' || to_char(cegv_db) || ' : ' || to_char(cegv_db - m003_r_db_for_cegv) || '.');
            end if;
            eddig:=52;      
        else
            dbms_output.put_line('A mai nap folyam�n nem ker�lt Q01-es c�g lev�logat�sra.');
        end if;
        --insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','6. Els� mell�klet k�sz'||to_char(sysdate,'YYYYMMDD HH24:MI:SS'));
--Ha nincsen Q01 sor (mert pl kiesett mind a ciklus belsej�ben!)
--Ha az elej�n m�r betettem a nemleges sort, akkor 1 db lesz, azaz itt nem ker�l be m�g egyszer.
        if levalogatva_1=0 then
		     eddig:=53;
		      sor:='Q01,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
			  eddig:=54;
              $if $$debug_on $then       
                  insert into vb_rep.mnb_napi_debug
                    values ('Q01',q01mellekletnev,1,sor);
              $else              
                  insert into vb_rep.mnb_napi
                    values ('Q01',q01mellekletnev,1,sor);
              $end  
			  levalogatva_1:=1;
			  eddig:=55;
		end if;	  
        
		
        for i in (select 
                        rownum rn, m003_je,  m003_ju, mv07, mv501_je, mv501_ju, dtol, jeju_r, kulf_ju, orszagkod,lezarva
                           from (select m003_je,to_char(m003_ju) m003_ju,mv07,mv501_je,mv501_ju,dtol,jeju_r,'0' kulf_ju, null orszagkod,decode(dig,null,'0','1') lezarva from
                                 vb.f003_juje,vb.f003 where f003.m003=f003_juje.m003_je
                                             and  (jeju_r  >= utolso_futas or datum_r>utolso_futas)
                                             and  substr(m0491,1,2)!='23' and m0491!='961' and m0491!='811'
--                        and
--                           (m040 not in ('0','9') or to_char(m040k,'YYYY')=to_char(sysdate,'YYYY'))
                union
                        select m003_je,to_char(m003_ju),mv07,mv501_je,mv501_ju,dtol,jeju_r,'0' kulf_ju, null orszagkod,decode(dig,null,'0','1')
                        from f003_juje, (select m003,param_dtol from vb_rep.vb_app_init where m003 is not null) c
                        where c.m003=f003_juje.m003_je and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas  -- �l�, vagy az adott napt�ri �vben sz�nt meg
                union   --a jogut�d n�lk�l megsz�nt, de k�lf�ldi jogut�dos szervezeteket is hozz� teszi a jeju rekordokhoz
                        select m003_je,'00000001' m003_ju,null mv07,					   
						decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') mv501_je,
					    decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') mv501_ju,
					    g.m040k dtol,j.datum_r,kulf_ju, null orszagkod,'0' from vb_ceg.jogutod j, vb.f003 g
                        where kulf_ju='1' and g.m003=m003_je and g.m040k_r>=utolso_futas and g.m040='9'
				union  -- a vb_app_init-be f�lvett t�rzssz�mok k�z�l a k�lf�ldi jogut�dos megsz�n�sek
                       select  distinct m003_je,'00000001' m003_ju,null mv07,
						decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') mv501_je,
					    decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') mv501_ju,
					   r.m040k dtol,
                       j.datum_r,kulf_ju,null orszagkod,'0' from vb_ceg.jogutod j, vb_rep.vb_app_init g, vb.f003 r
                        where kulf_ju='1' and g.m003=m003_je 
                        and g.m003=r.m003 and datum_r=(select max(datum_r) from vb_ceg.jogutod where m003_je=j.m003_je)
                        and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas))				
                --
/*				union      
					    select  f003_juje.*
                        from f003_juje, (select m003,param_dtol from vb_rep.vb_app_init where m003 is not null) c
                        where c.m003=f003_juje.m003_ju and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas
                union
                        select f003_juje.* from vb.f003_juje,vb.f003
                        where f003.m003=f003_juje.m003_ju
                        and  jeju_r  >= utolso_futas
                        and
                        substr(m0491,1,2)!='23' and m0491!='961' and m0491!='811'
						)) */
        loop
			sor:='Q02,'||
			   to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',E,KSHJEJU,@KSHJEJU'||
			   substr(to_char(sorok1+i.rn,'0999999'),2,7)||','||
			   to_char(i.m003_je)||','||
			   i.m003_ju||','||
			   i.mv07||','||
			   i.mv501_je||','||
			   i.mv501_ju||','||
			   to_char(i.dtol,'YYYYMMDD')||','||
			   to_char(i.jeju_r,'YYYYMMDD')||','||
			   i.kulf_ju||','||
			   i.orszagkod||','||
			   i.lezarva;
			   eddig:=56;
	--insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','6. M�sodik mell�klet k�sz');
	--commit;
	        if i.kulf_ju='1' then
			    kulf_db:=kulf_db+1;
			end if;
			begin
				   insert into  $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end (kod,filename,sorszam,rekord)
					   values ('Q02',q02mellekletnev,sorok1+i.rn,sor);                    
			exception
				when others then
					serr:=sqlcode;
					errmsg:=substr(sqlerrm,1,100);
					insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,errmsg);
					commit;
			end;  
        end loop;
		commit; 
		select count(*) into levalogatva_2 from  $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end  where kod='Q02' and 
		substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');
		eddig:=57;
		if levalogatva_2=0 then          
	  --nincs m�sodik mell�klet
			begin
				sor:='Q02,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
				insert into $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end
						values ('Q02',q02mellekletnev,1,sor);
				levalogatva_2:=0;--1 helyett nulla		
			exception
				when others then
					  serr:=sqlcode;
					  errmsg:=substr(sqlerrm,1,100);
					  insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,errmsg);
			end;         
			commit;
		end if;  --q02db+sorok1
		eddig:=58;
		--dbms_output.put_line('A m�sodik sz�m az �j t�rzssz�mok n�lk�li v�ltoz�sok sz�ma');
        --dbms_output.new_line;
        dbms_output.put_line('---');
        if(levalogatva_2 != 0) then
            dbms_output.put_line('Lev�logatott Q02-es jogel�d-jogut�d c�gp�rok: ' || to_char(levalogatva_2) || ', amelyek k�z�l k�lf�ldi jogut�d: ' || to_char(kulf_db) || '.');	
        else
            dbms_output.put_line('A mai nap folyam�n nem ker�lt Q02-es c�gp�r lev�logat�sra.');
        end if;
        
		--dbms_output.put_line('   amelyb�l k�lf�ldi jogut�d: '||to_char(kulf_db));
		begin
			update vb_rep.vb_app_init 
				 set 
				 param_ertek=datum_kar  --az utols� fut�s d�tuma
			where 
				 alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
			and  program='mnb_EBEAD.sql' 
			and  $if $$debug_on $then param_nev='utolso_futas - debug' $else param_nev='utolso_futas' $end
			and   sysdate>to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS'); 
			--update vb_rep.vb_app_init set m003=null;
			commit;
		exception when others then
			serr:=sqlcode;
			insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,'app_init_update a hiba');
		end;
		rekordsorszam:=rekordsorszam-kihagyott;
		eddig:=59;
		begin
		   $if $$debug_on $then
			vb.mod_szam_tolt('K865',tablaneve,rekordsorszam,'MNB napi v�ltoz�slista k�ld�se, debug m�d  Kihagyott: '||to_char(kihagyott),programneve||verzio,datum,'V');
		   $else
		     vb.mod_szam_tolt('K865',tablaneve,rekordsorszam,'MNB napi v�ltoz�slista k�ld�se  Kihagyott: '||to_char(kihagyott),programneve||verzio,datum,'V');
           $end		   
		exception when others then
			insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||datum_kar,'mod_szam_tolt a hiba');
		end;
		eddig:=60;
		commit;
exception
    when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,'A program eg�sz�t �rint� hiba: valami lev�logat�si hiba van a '||to_char(eddig)||' sz�mn�l a'||to_char(w_m003)||' t�rzssz�mon');
        commit;
        dbms_output.put_line(to_char(serr)||'  valami lev�logat�si hiba t�rt�nt '||to_char(eddig)||' sz�mn�l a'||to_char(w_m003)||' t�rzssz�mon');
        $if $$debug_on $then
		   vb.mod_szam_tolt('K865',tablaneve,rekordsorszam,'DEBUG M�dban HIB�VAL �RT V�GET '||to_char(eddig)||' sz�mn�l a '||to_char(w_m003)||' t�rzssz�mon:'||to_char(serr),programneve||verzio,datum,'V');
        $else
		   vb.mod_szam_tolt('K865',tablaneve,rekordsorszam,'HIB�VAL �RT V�GET '||to_char(eddig)||' sz�mn�l a '||to_char(w_m003)||' t�rzssz�mon:'||to_char(serr),programneve||verzio,datum,'V');
		$end
		commit;
end;
/                                                       

--alter session set plsql_ccflags='debug_on:false';
/*
prompt duplik�lt t�rzssz�mok
select substr(rekord,60,8),count(*) from vb_rep.mnb_napi where kod='Q01' group by substr(rekord,60,8) having count(*)>1;
prompt duplik�lt sorsz�mok
select substr(rekord,43,16),count(*) from vb_rep.mnb_napi where kod='Q01' group by substr(rekord,43,16) having count(*)>1;
prompt van-e felt�ltetlen el�z� �llom�ny m�g?
select substr(rekord,5,8),count(*) from vb_rep.mnb_napi where kod='Q01' group by substr(rekord,5,8) order by 1;
*/
--set termout on
--set linesize 80
--set trimspool on
--set pagesize 30
--set heading on
--set echo on

--exit;




