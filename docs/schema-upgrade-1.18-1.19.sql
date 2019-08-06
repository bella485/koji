-- upgrade script to migrate the Koji database schema
-- from version 1.18 to 1.19


BEGIN;

-- add compressed iso-compressed, vhd-compressed, vhdx-compressed, and vmdk-compressed
insert into archivetypes (name, description, extensions) values ('iso-compressed', 'Compressed iso image', 'iso.gz iso.xz');
insert into archivetypes (name, description, extensions) values ('vhd-compressed', 'Compressed VHD image', 'vhd.gz vhd.xz');
insert into archivetypes (name, description, extensions) values ('vhdx-compressed', 'Compressed VHDx image', 'vhd.gz vhd.xz');
insert into archivetypes (name, description, extensions) values ('vmdk-compressed', 'Compressed VMDK image', 'vmdk.gz vmdk.xz');

-- add kernel-image and imitramfs
insert into archivetypes (name, description, extensions) values ('kernel-image', 'Kernel BZ2 Image', 'vmlinuz vmlinuz.gz vmlinuz.xz');
insert into archivetypes (name, description, extensions) values ('initramfs', 'Compressed Initramfs Image', 'img');
COMMIT;
