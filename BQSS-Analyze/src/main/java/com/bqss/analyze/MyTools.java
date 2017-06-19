package com.bqss.analyze;


/**
 * Created by darrenfantasy on 2017/6/1.
 */
public class MyTools {
    public static String getAppNameByAppId(String AppId) {
        String name = "";
        if (AppId.equals("01c4f9c365f9407f904068ceb82b3cf6")) {
            name = "好周道";
        } else if (AppId.equals("04c25dfdb74d4b9b912862a2bde1d11d")) {
            name = "QQ输入法";
        } else if (AppId.equals("1c3f0cc2f7764337996987af6173fbc7")) {
            name = "阿里星球";
        } else if (AppId.equals("32902b123b0943ae9ffbb257913aaa20")) {
            name = "一周CP";
        } else if (AppId.equals("50c160047a5d48bb85b3d560977f251e")) {
            name = "百度输入法";
        } else if (AppId.equals("54eb8db5f6cb4e5e8bdfa3ed7891b651")) {
            name = "表情搜搜官网";
        } else if (AppId.equals("5613144ffde64d1d9be5d8b93998e90a")) {
            name = "钉钉";
        } else if (AppId.equals("63d0189963ac4c9a8e51a41734a47c87")) {
            name = "tataUFO";
        } else if (AppId.equals("66afd01e903f41a1a119b179ae3cdf0f")) {
            name = "微醺";
        } else if (AppId.equals("1247d6b11ae949779e7a0c85f0cef4af")) {
            name = "陌陌";
        } else if (AppId.equals("6e1823826610458894c81eac2df025c3")) {
            name = "粉粉日记";
        } else if (AppId.equals("7974ba7888a04ce8a7fa3f48e342c0b9")) {
            name = "iLoka.me";
        } else if (AppId.equals("89adbecf5f4f40949df3cfea0db348f8")) {
            name = "表情搜搜微信小程序";
        } else if (AppId.equals("9ca9cce006554107a32531e9330bf9ba")) {
            name = "磁场";
        } else if (AppId.equals("d9ec27aaa09a489a92573db38fdccfdd")) {
            name = "too";
        } else if (AppId.equals("ec176e29e1cc46e6a0847986d95c5979")) {
            name = "QQ空间";
        } else if (AppId.equals("f0a99c09eccb45d2892cb33b5ff98048")) {
            name = "飞信";
        } else if (AppId.equals("2701a32e83594cc284cf4ee4b61d5ba1")) {
            name = "小恩爱";
        } else if (AppId.equals("b19657b521f34b52bbc94d5f84995f64")) {
            name = "探探";
        } else if (AppId.equals("4f6896797d4045b3b453f554a60d275a")) {
            name = "淘宝&旺旺";
        } else if (AppId.equals("66e7667de09545a89ff7bd630cd18f53")) {
            name = "支付宝小程序";
        } else if (AppId.equals("8b3e11aa98df4c52835ee0f408903fc6")) {
            name = "QQ";
        } else if (AppId.equals("b88fe28f2bd7447b9e834bf093ef22f2")) {
            name = "斗图机小程序";
        }else {
            name = AppId;
        }
        return name;
    }
}
