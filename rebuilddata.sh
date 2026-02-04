rm -rf /home/admin/data
rm -rf data
rm etc/*bin
rm log/*
mkdir /home/admin/data
for disk in {1..8}; do mkdir -p /home/admin/data/ts_data/$disk; done;
for disk in {1..8}; do mkdir -p /home/admin/data/$disk; done; 
mkdir -p /home/admin/yaobase/data 
mkdir -p /home/admin/yaobase/data/as 
mkdir -p /home/admin/yaobase/data/as_commitlog
mkdir -p /home/admin/yaobase/data/ts_commitlog 
mkdir -p /home/admin/yaobase/data/ts_data/raid0 
mkdir -p /home/admin/yaobase/data/ts_data/raid1 
mkdir -p /home/admin/yaobase/data/ts_data/raid2 
mkdir -p /home/admin/yaobase/data/ts_data/raid3
ln -s /home/admin/data/ts_data/1 /home/admin/yaobase/data/ts_data/raid0/store0 
ln -s /home/admin/data/ts_data/2 /home/admin/yaobase/data/ts_data/raid0/store1 
ln -s /home/admin/data/ts_data/3 /home/admin/yaobase/data/ts_data/raid1/store0 
ln -s /home/admin/data/ts_data/4 /home/admin/yaobase/data/ts_data/raid1/store1 
ln -s /home/admin/data/ts_data/5 /home/admin/yaobase/data/ts_data/raid2/store0 
ln -s /home/admin/data/ts_data/6 /home/admin/yaobase/data/ts_data/raid2/store1 
ln -s /home/admin/data/ts_data/7 /home/admin/yaobase/data/ts_data/raid3/store0
ln -s /home/admin/data/ts_data/8 /home/admin/yaobase/data/ts_data/raid3/store1
for disk in {1..8}; do mkdir -p /home/admin/data/$disk/obtest/sstable; done; 
for disk in {1..8}; do ln -s /home/admin/data/$disk /home/admin/yaobase/data/$disk; done; 
