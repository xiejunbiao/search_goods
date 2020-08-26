# -*- coding: utf-8 -*-
"""
Created on Wed May 27 16:26:13 2020

@author: lijiangman
"""


# UPDATE cb_hot_search_words_copy_update SET hot_search_words ='香蕉' WHERE UUID='1';


a_col="a.spu_code, a.spu_name, a.goods_short_edit\
    ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
    ,a.spu_cate_third, a.spu_cate_third_edit\
    ,a.sale_price, a.sale_month_count, a.shop_name"
select_spu_table="""
        a_col="a.spu_code, a.spu_name, a.goods_short_edit\
            ,a.goods_brand, a.spu_cate_first, a.spu_cate_second\
            ,a.spu_cate_third, a.spu_cate_third_edit\
            ,a.sale_price, a.sale_month_count, a.shop_name"    
"""

add_column="""
ALTER TABLE cb_goods_spu_search_update ADD COLUMN `updated_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间';

"""

insert_spu_table="""
INSERT ignore INTO cb_goods_spu_search_test
( spu_code, spu_name, goods_short_edit, spu_cate_first, spu_cate_second, spu_cate_third, sale_price, sale_month_count, shop_name, shop_code, goods_status, prod_area, store_status, promote)
VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') ;
"""

drop_spu_table="""
DROP TABLE cb_goods_spu_search_test
"""

drop_table="""
DROP TABLE cb_hot_search_words_copy_update
"""

create_table="""
CREATE TABLE `cb_hot_search_words_copy_update` (
  `autoid` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `uuid` VARCHAR(40) NOT NULL COMMENT 'UUID',
  `sort_number` INT(8) DEFAULT NULL COMMENT '排序',
  `area_code` VARCHAR(40) NOT NULL COMMENT '小区ID',
  `hot_search_words` VARCHAR(40) NOT NULL COMMENT '搜索推广词',
  `hot_search_times` INT(11) DEFAULT '0' COMMENT '搜素次数',
  `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` VARCHAR(40) DEFAULT NULL COMMENT '创建人',
  `updated_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `updated_by` VARCHAR(40) DEFAULT NULL COMMENT '更新人',
  PRIMARY KEY (`autoid`),
  UNIQUE KEY `area` (`area_code`,`hot_search_words`)
) ENGINE=INNODB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COMMENT='社区云脑搜素词配置表'

"""
create_spu_table="""
CREATE TABLE `cb_goods_spu_search_test` (
  `autoid` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `spu_code` varchar(40) NOT NULL COMMENT 'spu编码',
  `spu_name` varchar(400) NOT NULL COMMENT 'spu名称',
  `goods_short` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu名称关键字（商品简称）',
  `goods_short_edit` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu名称关键字（商品简称），算法人员编辑',
  `spu_name_synonym` varchar(40) NOT NULL DEFAULT '' COMMENT '同义商品（同义词），算法人员编辑',
  `spu_name_similar` varchar(40) NOT NULL DEFAULT '' COMMENT '相似商品（近义词），算法人员编辑',
  `goods_brand` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu品牌名称',
  `goods_brand_edit` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu品牌名称，算法人员编辑',
  `spu_sale_point` varchar(255) NOT NULL DEFAULT '' COMMENT 'spu卖点',
  `spu_cate_first` varchar(40) NOT NULL COMMENT 'spu一级分类名称',
  `spu_cate_first_edit` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu一级分类名称，算法人员编辑',
  `spu_cate_second` varchar(40) NOT NULL COMMENT 'spu二级分类名称',
  `spu_cate_second_edit` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu二级分类名称，算法人员编辑',
  `spu_cate_third` varchar(40) NOT NULL COMMENT 'spu三级分类名称',
  `spu_cate_third_edit` varchar(40) NOT NULL DEFAULT '' COMMENT 'spu三级分类名称，算法人员编辑',
  `sale_price` decimal(27,2) NOT NULL COMMENT 'spu最低售价',
  `sale_month_count` mediumint(8) unsigned NOT NULL COMMENT 'spu销量（最近30天）',
  `spu_ms` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'spu是否参与秒杀',
  `spu_yhq` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'spu是否有优惠券',
  `shop_name` varchar(40) NOT NULL COMMENT 'spu所属店铺名称',
  `shop_code` varchar(40) NOT NULL COMMENT 'spu所属店铺编码',
  `goods_status` tinyint(4) NOT NULL COMMENT '商品上下架状态，1上架，0下架',
  `spu_info_all` varchar(900) NOT NULL DEFAULT '' COMMENT '|分隔的spu组合信息，顺序spu_name goods_short goods_brand spu_sale_point spu_cate_first spu_cate_second spu_cate_third shop_name',
  `spu_info_all_edit` varchar(900) NOT NULL DEFAULT '' COMMENT 'spu组合信息，算法人员编辑',
  `prod_area` varchar(40) DEFAULT NULL COMMENT '产地',
  `store_status` int(11) DEFAULT NULL COMMENT '售罄状态',
  `promote` varchar(100) DEFAULT NULL COMMENT '活动名',
  `created_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `created_by` VARCHAR(40) DEFAULT NULL COMMENT '创建人',
  `updated_time` DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新时间',
  `updated_by` VARCHAR(40) DEFAULT NULL COMMENT '更新人',
  PRIMARY KEY (`autoid`),
  UNIQUE KEY `spu` (`spu_code`)
) ENGINE=InnoDB AUTO_INCREMENT=219005 DEFAULT CHARSET=utf8mb4 COMMENT='商品搜索数据表'

"""