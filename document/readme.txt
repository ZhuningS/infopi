以BSD 2-Clause许可协议发布

使用教程，可参考这个博客
http://www.cnblogs.com/infopi/


infopi.odg为LibreOffice的绘图文件
infopi.ods为LibreOffice的电子表格文件


demo_cfg.zip为演示用的配置文件，第一次使用时把它解压到infopi目录下。
解压后，进入infopi-master/cfg目录，应该能看到config.ini、admin.txt、还有几个目录，这些就是演示用的配置。



已知问题：
2038年后，由于unix时间戳的取值范围到达32位整数的极限，InfoPi会出现问题。
届时cookies将无法设置，内部处理数据时也有可能出问题。