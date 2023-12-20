Search.setIndex({docnames:["index","reference","reference/autosummary/plantdb.fsdb.FSDB","reference/autosummary/plantdb.fsdb.File","reference/autosummary/plantdb.fsdb.Fileset","reference/autosummary/plantdb.fsdb.Scan","reference/autosummary/plantdb.rest_api.File","reference/autosummary/plantdb.rest_api.Image","reference/autosummary/plantdb.rest_api.Mesh","reference/autosummary/plantdb.rest_api.PointCloud","reference/autosummary/plantdb.rest_api.PointCloudGroundTruth","reference/autosummary/plantdb.rest_api.Refresh","reference/autosummary/plantdb.rest_api.Scan","reference/autosummary/plantdb.rest_api.ScanList","reference/db","reference/fsdb","reference/io","reference/rest_api","reference/sshfsdb","reference/webcache","webapi"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":5,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,"sphinx.ext.intersphinx":1,"sphinx.ext.viewcode":1,sphinx:56},filenames:["index.md","reference.md","reference/autosummary/plantdb.fsdb.FSDB.md","reference/autosummary/plantdb.fsdb.File.md","reference/autosummary/plantdb.fsdb.Fileset.md","reference/autosummary/plantdb.fsdb.Scan.md","reference/autosummary/plantdb.rest_api.File.md","reference/autosummary/plantdb.rest_api.Image.md","reference/autosummary/plantdb.rest_api.Mesh.md","reference/autosummary/plantdb.rest_api.PointCloud.md","reference/autosummary/plantdb.rest_api.PointCloudGroundTruth.md","reference/autosummary/plantdb.rest_api.Refresh.md","reference/autosummary/plantdb.rest_api.Scan.md","reference/autosummary/plantdb.rest_api.ScanList.md","reference/db.md","reference/fsdb.md","reference/io.md","reference/rest_api.md","reference/sshfsdb.md","reference/webcache.md","webapi.md"],objects:{"plantdb.db":[[14,1,1,"","DB"],[14,3,1,"","DBBusyError"],[14,1,1,"","File"],[14,1,1,"","Fileset"],[14,1,1,"","Scan"]],"plantdb.db.DB":[[14,2,1,"","connect"],[14,2,1,"","create_scan"],[14,2,1,"","delete_scan"],[14,2,1,"","disconnect"],[14,2,1,"","get_scan"],[14,2,1,"","get_scans"]],"plantdb.db.File":[[14,4,1,"","db"],[14,4,1,"","filename"],[14,4,1,"","fileset"],[14,2,1,"","get_db"],[14,2,1,"","get_fileset"],[14,2,1,"","get_id"],[14,2,1,"","get_metadata"],[14,2,1,"","get_scan"],[14,4,1,"","id"],[14,2,1,"","import_file"],[14,2,1,"","read"],[14,2,1,"","read_raw"],[14,2,1,"","set_metadata"],[14,2,1,"","write"],[14,2,1,"","write_raw"]],"plantdb.db.Fileset":[[14,2,1,"","create_file"],[14,4,1,"","db"],[14,2,1,"","delete_file"],[14,2,1,"","get_db"],[14,2,1,"","get_file"],[14,2,1,"","get_files"],[14,2,1,"","get_id"],[14,2,1,"","get_metadata"],[14,2,1,"","get_scan"],[14,4,1,"","id"],[14,4,1,"","scan"],[14,2,1,"","set_metadata"]],"plantdb.db.Scan":[[14,2,1,"","create_fileset"],[14,4,1,"","db"],[14,2,1,"","delete_fileset"],[14,2,1,"","get_db"],[14,2,1,"","get_fileset"],[14,2,1,"","get_filesets"],[14,2,1,"","get_id"],[14,2,1,"","get_metadata"],[14,4,1,"","id"],[14,2,1,"","set_metadata"]],"plantdb.fsdb":[[15,1,1,"","FSDB"],[15,1,1,"","File"],[15,3,1,"","FileNoFileNameError"],[15,3,1,"","FileNoIDError"],[15,1,1,"","Fileset"],[15,3,1,"","FilesetNoIDError"],[15,3,1,"","FilesetNotFoundError"],[15,5,1,"","LOCK_FILE_NAME"],[15,5,1,"","MARKER_FILE_NAME"],[15,3,1,"","NotAnFSDBError"],[15,1,1,"","Scan"],[15,6,1,"","dummy_db"]],"plantdb.fsdb.FSDB":[[15,4,1,"","basedir"],[15,2,1,"","connect"],[15,2,1,"","create_scan"],[15,2,1,"","delete_scan"],[15,2,1,"","disconnect"],[15,2,1,"","get_scan"],[15,2,1,"","get_scans"],[15,4,1,"","is_connected"],[15,2,1,"","list_scans"],[15,4,1,"","lock_path"],[15,2,1,"","path"],[15,2,1,"","reload"],[15,4,1,"","scans"]],"plantdb.fsdb.File":[[15,4,1,"","db"],[15,4,1,"","filename"],[15,4,1,"","fileset"],[15,2,1,"","get_metadata"],[15,4,1,"","id"],[15,2,1,"","import_file"],[15,4,1,"","metadata"],[15,2,1,"","path"],[15,2,1,"","read"],[15,2,1,"","read_raw"],[15,2,1,"","set_metadata"],[15,2,1,"","store"],[15,2,1,"","write"],[15,2,1,"","write_raw"]],"plantdb.fsdb.Fileset":[[15,2,1,"","create_file"],[15,4,1,"","db"],[15,2,1,"","delete_file"],[15,4,1,"","files"],[15,2,1,"","get_file"],[15,2,1,"","get_files"],[15,2,1,"","get_metadata"],[15,4,1,"","id"],[15,2,1,"","list_files"],[15,4,1,"","metadata"],[15,2,1,"","path"],[15,4,1,"","scan"],[15,2,1,"","set_metadata"],[15,2,1,"","store"]],"plantdb.fsdb.Scan":[[15,2,1,"","create_fileset"],[15,4,1,"","db"],[15,2,1,"","delete_fileset"],[15,4,1,"","filesets"],[15,2,1,"","get_fileset"],[15,2,1,"","get_filesets"],[15,2,1,"","get_measures"],[15,2,1,"","get_metadata"],[15,4,1,"","id"],[15,2,1,"","list_filesets"],[15,4,1,"","metadata"],[15,2,1,"","path"],[15,2,1,"","set_metadata"],[15,2,1,"","store"]],"plantdb.io":[[16,6,1,"","fsdb_file_from_local_file"],[16,6,1,"","read_graph"],[16,6,1,"","read_image"],[16,6,1,"","read_json"],[16,6,1,"","read_npz"],[16,6,1,"","read_point_cloud"],[16,6,1,"","read_toml"],[16,6,1,"","read_torch"],[16,6,1,"","read_triangle_mesh"],[16,6,1,"","read_volume"],[16,6,1,"","read_voxel_grid"],[16,6,1,"","tmpdir_from_fileset"],[16,6,1,"","to_file"],[16,6,1,"","write_graph"],[16,6,1,"","write_image"],[16,6,1,"","write_json"],[16,6,1,"","write_npz"],[16,6,1,"","write_point_cloud"],[16,6,1,"","write_toml"],[16,6,1,"","write_torch"],[16,6,1,"","write_triangle_mesh"],[16,6,1,"","write_volume"],[16,6,1,"","write_voxel_grid"]],"plantdb.rest_api":[[17,1,1,"","Archive"],[17,1,1,"","File"],[17,1,1,"","Image"],[17,1,1,"","Mesh"],[17,1,1,"","PointCloud"],[17,1,1,"","PointCloudGroundTruth"],[17,1,1,"","Refresh"],[17,1,1,"","Scan"],[17,1,1,"","ScanList"],[17,6,1,"","compute_fileset_matches"],[17,6,1,"","get_path"],[17,6,1,"","get_scan_data"],[17,6,1,"","get_scan_date"],[17,6,1,"","get_scan_info"],[17,6,1,"","get_scan_template"],[17,6,1,"","list_scans_info"]],"plantdb.rest_api.Archive":[[17,2,1,"","get"]],"plantdb.rest_api.File":[[17,2,1,"","get"]],"plantdb.rest_api.Image":[[17,2,1,"","get"]],"plantdb.rest_api.Mesh":[[17,2,1,"","get"]],"plantdb.rest_api.PointCloud":[[17,2,1,"","get"]],"plantdb.rest_api.PointCloudGroundTruth":[[17,2,1,"","get"]],"plantdb.rest_api.Refresh":[[17,2,1,"","get"]],"plantdb.rest_api.Scan":[[17,2,1,"","get"]],"plantdb.rest_api.ScanList":[[17,2,1,"","get"]],"plantdb.sshfsdb":[[18,1,1,"","SSHFSDB"]],"plantdb.sshfsdb.SSHFSDB":[[18,4,1,"","basedir"],[18,2,1,"","connect"],[18,2,1,"","disconnect"],[18,4,1,"","is_connected"],[18,4,1,"","remotedir"],[18,4,1,"","scans"]],"plantdb.webcache":[[19,6,1,"","image_path"],[19,6,1,"","mesh_path"],[19,6,1,"","pointcloud_path"]],plantdb:[[14,0,0,"-","db"],[15,0,0,"-","fsdb"],[16,0,0,"-","io"],[17,0,0,"-","rest_api"],[18,0,0,"-","sshfsdb"],[19,0,0,"-","webcache"]]},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","exception","Python exception"],"4":["py","attribute","Python attribute"],"5":["py","data","Python data"],"6":["py","function","Python function"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:exception","4":"py:attribute","5":"py:data","6":"py:function"},terms:{"0":[16,20],"00":20,"00000_rgb":[19,20],"0013571157486977348":20,"007":15,"008":15,"01":[15,20],"02":[15,20],"038165890845442085":20,"04786630673761355":20,"0579549181538338":20,"06475585405884698":20,"07043190848918":20,"0x7f0730b1e390":15,"0x7f34fc860fd0":15,"1":[15,16,19,20],"10":16,"105":20,"1080":20,"10k":19,"1166":20,"12":17,"120":20,"127":20,"13":20,"1440":20,"15":17,"150":19,"1500":19,"1500x1500":19,"150x150":19,"15283":16,"16":17,"180":20,"2":[15,16,19,20],"200":20,"2019":20,"2023":17,"255":16,"2d":14,"3":[15,16,19,20],"330":20,"3390191175518756":20,"340":20,"34181295964290737":20,"35":20,"36109311437637":20,"369":20,"37":17,"3x1":20,"3x3":20,"4":16,"410":20,"42":20,"4279687732083":20,"440":20,"48994":16,"5":16,"50":16,"500":20,"5000":20,"540":20,"5x5":16,"60":17,"62":20,"6fbae08f195837c511af7c2864d075dd5cd153bc":[19,20],"720":20,"77e25820ddd8facd7d7a4bc5b17ad3c81046becc":19,"8":19,"8bit":16,"9385481965778085":20,"9389926865509284":20,"9518889440105":20,"99":15,"9971710205080586":20,"abstract":[0,1,15],"byte":[14,15],"class":[0,1,16,18],"default":[14,15,16,19,20],"do":15,"final":20,"float":[19,20],"function":[16,19],"import":[14,15,16,17,18,19],"int":14,"new":[14,15],"null":20,"return":[14,15,16,17,19,20],"true":[15,16,17,18,20],"try":17,A:[0,14,15,16,17,19,20],And:15,BUT:15,For:[0,20],If:[14,15],In:20,It:[14,15,20],NOT:15,No:15,The:[0,14,15,16,17,18,19,20],These:15,To:[0,15],__init__:[2,3,4,5,6,7,8,9,10,11,12,13],_delete_scan:15,_file_metadata_path:15,_fileset_metadata_json_path:15,_filter_queri:15,_is_valid_id:15,_make_fileset:15,_make_scan:15,_scan_metadata_path:15,about:[0,20],absolut:[15,17],access:[0,14,20],acquisit:17,acquisition_d:17,actual:14,ad:[15,18],add:[15,19,20],address:15,all:[14,15,16,20],allow:15,alreadi:15,also:20,alwai:20,an:[14,15,16,17,19,20],anaconda:0,angl:20,angleandinternod:20,anglesandinternod:20,ani:[14,15,19],api:[0,14],ar:[14,15,19,20],arabidopsi:20,archiv:17,argument:20,arrai:[16,20],as_view:[6,7,8,9,10,11,12,13],asarrai:16,assert_array_equ:16,assign:15,associ:[0,14,15,16],assum:15,attach:15,attribut:[6,7,8,9,10,11,12,13,15],autoclass:[2,3,4,5,6,7,8,9,10,11,12,13],automatedmeasur:20,automethod:[2,3,4,5,6,7,8,9,10,11,12,13],autosummari:[2,3,4,5,6,7,8,9,10,11,12,13],avail:[0,19],b:15,base:15,basedir:[15,18],been:15,below:15,binari:16,bond:15,bool:[14,15,16,18],bound:20,box:20,buffer:14,busi:14,bytearrai:14,cach:[17,19],call:15,camera:17,can:[0,14,20],cannot:[14,15],chach:19,chang:15,channel:0,check:15,cli:[17,20],cloud:[17,19,20],code:0,colmap:20,com:[0,18],combin:19,commun:[0,14,20],complet:20,compliant:20,compress:16,compute_fileset_match:17,concret:17,connect:[2,14,15,16,17,18,19],consid:15,contain:[14,15,16,18,20],content:[15,20],contrari:15,control:20,convent:20,convert:[15,16],correspond:20,could:15,creat:[14,15,16,19],create_fil:[4,14,15,16],create_fileset:[5,14,15],create_scan:[2,14,15],creation:17,credenti:14,cuda:16,current:15,currentmodul:[2,3,4,5,6,7,8,9,10,11,12,13],curveskeleton:[17,20],curveskeleton__trianglemesh_0393cb5708:17,cx:20,cy:20,data:[14,15,16,17,18,19,20],databas:[0,1,16,17,19,20],dataset:[15,17,20],date:[17,20],datetim:17,db:[14,15,16,17,18,19],db_locat:17,db_prefix:17,dbbusyerror:[14,15],dbfile:16,dbroot:15,decor:[6,7,8,9,10,11,12,13],dedic:15,deduc:14,default_rng:16,defin:[14,15],delet:[14,15],delete_fil:[4,14,15],delete_fileset:[5,14,15],delete_scan:[2,14,15],deseri:16,detail:[0,1],dict:[14,15,16,17],dictionari:[15,16,17,20],did:15,differ:15,directori:[15,16,17,18,19,20],disconnect:[2,14,15,17,18,19],disk:15,dispatch_request:[6,7,8,9,10,11,12,13],distinguish:14,doe:14,done:[14,20],down:[17,19],downsampl:19,downsiz:19,drive:15,dtype:16,dummi:15,dummy_db:[15,16,17],dummy_imag:15,dump:15,duplic:15,e:16,each:15,edg:16,els:[15,17,18],empti:15,encod:15,entri:[15,20],environ:[15,17,19,20],equal:16,error:[14,17,20],eu:0,exampl:[15,16,17,18,19,20],except:[14,15,16],exist:[14,15],ext:[14,15,16],extens:[14,15,16],extrins:20,f:[15,16,17],f_lzw:16,fail:20,fals:[14,15,16,17,18,20],field:20,fil:15,file:[0,1,14,17,19],file_007:15,file_id:[14,15,17,19],filenam:[14,15],filenofilenameerror:15,filenoiderror:15,fileset:[14,15,16,17,19],fileset_001:[15,16,17],fileset_id:[14,15,17,19],filesetnoiderror:15,filesetnotfounderror:15,filesuri:[17,20],filesystem:16,filter:[15,17],filterqueri:20,find:[14,15],first:[18,20],flask:[17,20],folder:15,follow:[14,15,19,20],forc:17,format:[14,17,18],formerli:20,found:[15,18],from:[0,14,15,16,17,18,19,20],fs:[15,16],fs_007:15,fsbd:15,fsdb:[15,16,17,18,19,20],fsdb_file_from_local_fil:16,fsdb_rest_api:[17,20],fx:20,fy:20,g2:16,g:16,gather:[15,20],gener:0,geometri:16,get:[6,7,8,9,10,11,12,13,14,15,17,19,20],get_db:[3,4,5,14],get_fil:[4,14,15,16],get_fileset:[3,5,14,15,16],get_id:[3,4,5,14],get_measur:[5,15],get_metadata:[3,4,5,14,15],get_path:17,get_scan:[2,3,4,14,15,16,17],get_scan_d:17,get_scan_data:17,get_scan_info:17,get_scan_templ:[17,20],ghostbust:15,given:[14,15,19],global:17,goe:15,gonna:15,grayscal:16,ground:[17,20],group:20,ha:15,handl:[15,18],hasangledata:[17,20],hasautomatedmeasur:[17,20],hasmanualmeasur:[17,20],hasmesh:[17,20],haspcdgroundtruth:[17,20],haspointcloud:[17,20],haspointcloudevalu:[17,20],hassegmentation2d:[17,20],hassegmentedpcdevalu:[17,20],hassegmentedpointcloud:[17,20],hasskeleton:[17,20],hastreegraph:[17,20],have:[0,15],height:[19,20],helper:16,here:[0,20],hereaft:[16,20],host:[15,16],how:[0,20],howev:15,http:[0,17,20],hub:0,i:[0,1],id:[14,15,17,19],identifi:19,imag:[14,15,17,19],image_id:20,image_path:19,imageio:16,img2:16,img:[15,16],implement:[14,15,18,20],import_fil:[3,14,15],index:20,indic:[16,20],indoor:17,info:[17,20],inform:[17,20],init:[2,3,4,5,6,7,8,9,10,11,12,13],init_every_request:[6,7,8,9,10,11,12,13],initi:15,instanc:[15,17],interest:15,interfac:[14,19],internod:20,intrins:20,introduc:20,introduct:0,invalid:15,io:[15,16],ioerror:15,is_connect:[15,18],ismatch:20,iterdir:15,its:[15,20],jame:15,join:15,jpeg:[16,19,20],jpg:[15,16,19,20],js:15,js_dict:15,json:[15,17,20],just:15,k1:20,k2:20,k:20,kei:[14,15],known:20,larg:[19,20],latest:15,learn:0,librari:[0,15,16],like:16,list:[14,15,16,17,18,20],list_fil:[4,15],list_fileset:[5,15],list_scan:[2,15],list_scans_info:17,listdir:15,load:[15,16],local:[0,1,16,18],locat:15,lock:[15,18],lock_file_nam:[15,18],lock_path:15,login_data:[14,15,18],look:0,lyon:17,made:14,mai:17,main:15,manual:[15,20],map:17,marker:15,marker_file_nam:15,mask:20,masks_1__0__1__0____channel____rgb_5619aa428d:20,match:17,matrix:20,max:[19,20],md:15,mean:16,measur:[15,20],measured_angl:20,measured_internod:20,mesh:[17,19],mesh_path:19,messag:[14,15],metadata:[14,15,17],method:[1,2,3,4,5,6,7,8,9,10,11,12,13],method_decor:[6,7,8,9,10,11,12,13],microfarm:16,miss:20,mode:15,model:17,modul:[0,1,14,15,16,17,18,20],more:[0,20],mount:18,must:15,myscan_001:[15,16,17],myscan_002:15,n:[17,20],name:[15,17,20],nbphoto:[17,20],ndarrai:16,necessari:17,need:15,networkx:16,new_f:15,new_fil:15,new_fileset:15,new_scan:15,node:16,none:[14,15,17,18,20],nonetyp:15,notanfsdberror:15,note:[14,15],now:15,np:16,npz2:16,npz:16,number:20,numpi:16,nx:16,o3d:16,o:[0,1],object:[14,15,16,18,19,20],obtain:[15,20],obvious:19,one:15,ones:16,onli:[15,16],open3d:16,open:15,opencv:20,oper:[0,1,14],option:[14,15,16,17,18,19],org:0,organ:20,orig:[19,20],origin:[19,20],os:[15,19],oserror:15,other:[15,20],outfil:15,output:[15,20],p1:20,p2:20,p:[15,16,17],param:20,paramet:[14,15,16,17,18,19,20],parent:14,path:[2,3,4,5,14,15,16,17,18,19],path_graph:16,pathlib:[15,19],pcd:16,photouri:20,pictur:[14,20],plant:[17,20],plantdb:[14,15,16,17,18,19],pleas:0,ply:[16,17,19,20],png:[15,16,20],point:[17,19,20],pointcloud:[16,17,19],pointcloud_1_0_0_0_10_0_ca07eb2790:19,pointcloud_1_0_1_0_10_0_7ee836e5a9:[17,19,20],pointcloud_path:19,pointcloudevalu:20,pointcloudgroundtruth:[17,20],posixpath:[17,19],possibl:15,prefix:17,presenc:20,present:[15,20],preserv:19,prevent:15,preview:[19,20],print:[15,16,17,18],project:[0,14],provid:19,provide_automatic_opt:[6,7,8,9,10,11,12,13],pt:16,pull:0,pybind:16,python:[0,16,20],queri:[15,17],r:15,rais:[14,15,16],random:[15,16],rang:16,raw:15,read:[3,14,15,16],read_graph:16,read_imag:[15,16],read_json:16,read_npz:16,read_point_cloud:16,read_raw:[3,14,15],read_toml:16,read_torch:16,read_triangle_mesh:16,read_volum:16,read_voxel_grid:16,reader:15,real_plant_analyz:[17,19,20],reconnect:15,reconstruct:20,refer:20,referenc:15,refresh:17,regroup:17,rel:17,relat:17,reload:[2,15,17],remesh:19,remot:[0,1],remotedir:18,remov:[15,18],repres:[14,20],represent:[6,7,8,9,10,11,12,13,15],request:[17,19],requir:[15,20],resiz:[17,19,20],resourc:[17,19,20],respons:[17,20],rest:[0,1],rest_api:[17,20],retriev:[14,15],rgb:[14,16],rgba:16,right:15,rng:16,robot:16,romi:[0,14,15,16,19],romi_db:[15,17,19,20],romidb:15,romidb_:15,romidb_j0pbkoo0:15,root:[15,20],rotat:20,rotmat:20,rubric:[2,3,4,5,6,7,8,9,10,11,12,13],s:15,same:[15,20],sampl:[17,19],sango36:19,sango_90_300_36:[15,19],save:[14,15,16,20],scan:[0,14,15,16,17,18,19],scan_data:17,scan_id:[17,19],scan_img_01:15,scan_img_02:15,scan_img_99:15,scan_info:17,scanlist:[17,20],scans_info:17,search:20,see:[15,20],segmentation2d:20,segmentation2devalu:20,segmentedpointcloud:20,select:20,send:17,separ:15,serv:17,server:[17,18,20],set:[14,15,17],set_metadata:[3,4,5,14,15],sever:14,should:[14,15,16,18,20],simpl:15,simple_radi:17,singl:[16,17,20],size:[19,20],skeleton:[17,20],some:[15,17,20],someon:18,sourc:[0,14,15,16,17,18,19],speci:[17,20],specif:[19,20],ssh:[0,1],sshf:18,sshfsdb:18,st_size:16,start:20,stat:16,still:15,store:[3,4,5,15],str:[14,15,16,17,18,19],string:[15,17,20],structur:[15,18],stupid:16,subclass:[14,18],summari:0,system:[0,1],task:[17,20],task_a:15,templat:[17,20],temporari:[15,16],test:[15,16,20],test_databas:[17,19],test_imag:[15,16],test_json2:15,test_json:[15,16],test_npz:16,test_nx_graph:16,test_volum:16,test_volume_compress:16,text:14,thei:15,thi:[0,14,15,17,18,19,20],those:20,three:19,thumb:[19,20],thumbnail:[19,20],thumbnail_pict20190201_134037_0:20,thumbnailuri:17,tiff:16,time:[15,17],tmp:[15,17,19,20],tmpdir_from_fileset:16,to_fil:16,todo:19,torch:16,torchtensor:16,train:16,translat:20,tree:[17,20],treegraph:17,treegraph__false_curveskeleton_c304a2cc71:17,trianglemesh:[16,17,19,20],trianglemesh_9_most_connected_t_open3d_00e095c359:[17,19,20],triangular:[16,17],truth:[17,20],tvec:20,type:[14,15,16,18],uint8:16,under:[15,20],uniqu:15,unknown:15,unknown_f:15,unknown_scan:15,unsaf:15,unus:[15,18,19],updat:15,upon:17,uri:20,url:0,us:[0,14,15,16,17,18,19,20],user:18,util:[16,19],valid:15,valu:[14,15,16,20],variabl:17,vector3dvector:16,vector:20,version:[15,19],via:20,virtual_plant_analyz:20,visual:20,vol2:16,vol:16,voxel:[19,20],voxelgrid:16,we:[16,20],webapp:20,webcach:[0,1,20],websit:0,when:[14,15,20],where:[14,15,18],which:17,who:15,width:[19,20],with_fil:15,with_fileset:[15,16,17],with_scan:15,within:0,word:20,work:19,workspac:20,write:[3,14,15,16],write_graph:16,write_imag:16,write_json:[15,16],write_npz:16,write_point_cloud:16,write_raw:[3,14,15],write_toml:16,write_torch:16,write_triangle_mesh:16,write_volum:16,write_voxel_grid:16,writen:15,written:16,wrong_f:15,x:[15,20],y:20,you:15,z:20,zip:20},titles:["Welcome to plantdb\u2019s reference documentation!","Reference API","plantdb.fsdb.FSDB","plantdb.fsdb.File","plantdb.fsdb.Fileset","plantdb.fsdb.Scan","plantdb.rest_api.File","plantdb.rest_api.Image","plantdb.rest_api.Mesh","plantdb.rest_api.PointCloud","plantdb.rest_api.PointCloudGroundTruth","plantdb.rest_api.Refresh","plantdb.rest_api.Scan","plantdb.rest_api.ScanList","Abstract database class","Local file system database","I/O operations","REST API","Remote file system database with SSH","Webcache module","plantDB REST API"],titleterms:{"2d":16,"3d":16,"abstract":14,"class":[14,15,17],api:[1,17,20],archiv:20,camera:20,cloud:16,conda:0,databas:[14,15,18],detail:[15,16,17,19,20],docker:0,document:0,file:[3,6,15,16,18,20],file_id:20,fileset:4,fileset_id:20,format:16,fsdb:[2,3,4,5],github:0,graph:16,grid:16,ha:20,i:16,id:20,imag:[0,7,16,20],json:16,label:16,local:15,mesh:[8,16,20],metadata:20,method:[15,17,19],model:20,modul:19,o:16,oper:16,packag:0,path:20,pcgroundtruth:20,plantdb:[0,2,3,4,5,6,7,8,9,10,11,12,13,20],point:16,pointcloud:[9,20],pointcloudgroundtruth:10,pose:20,pytorch:16,refer:[0,1],refresh:[11,20],remot:18,repositori:0,rest:[17,20],rest_api:[6,7,8,9,10,11,12,13],s:0,scan:[5,12,20],scan_id:20,scanlist:13,ssh:18,summari:20,system:[15,18],tensor:16,thumbnailuri:20,toml:16,tree:16,triangl:16,url:20,volum:16,voxel:16,webcach:19,welcom:0}})