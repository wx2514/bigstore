package com.other;


import com.alibaba.fastjson.util.ParameterizedTypeImpl;

import java.lang.reflect.Type;

/**
 * Created by wuxiang on 16-6-22.
 */
public class TypeReferenceMogu<T> extends com.alibaba.fastjson.TypeReference<T> {
    public Type type;

    public Type getType() {
        return type;
    }

    protected TypeReferenceMogu(String str) throws Exception {
        this(str, 0);
        /*Type superClass = this.getClass().getGenericSuperclass();
        this.type = ((ParameterizedType)superClass).getActualTypeArguments()[0];*/
    }

    private TypeReferenceMogu(String str, int depth) throws Exception {
        if (depth > 10)
            throw new Exception("recursive depth for TypeReferenceMogu is too deep, larger than 10");

        str = str.replaceAll(" ", "");
        int start = str.indexOf("<");
        int end = str.lastIndexOf(">");
        Type rawType = null;
        Type[] actualTypeArguments = {};
        if (start > -1 && end > -1) {
            String rawName = str.substring(0, start);
            rawType = Class.forName(rawName);
            String actualTypeStr = null;
            actualTypeStr = str.substring(start + 1, end);
            String[] actualTypeArray = split(actualTypeStr, ',');
            actualTypeArguments = new Type[actualTypeArray.length];
            for (int i = 0, k = actualTypeArray.length; i < k; i++) {
                actualTypeArguments[i] = new TypeReferenceMogu(actualTypeArray[i], ++depth).getType();
            }
        } else {
            rawType = Class.forName(str);
        }
        type = new ParameterizedTypeImpl(actualTypeArguments, null, rawType);
    }

    private static String[] split(String sp, char split) {
        String[] result = null;
        int index = -1;
        int foundCount = 0;
        byte[] arrInput  = sp.getBytes();
        for(int i = 0;i<arrInput.length;i++) {
            if (arrInput[i] == '<') {
                foundCount++;
            } else if (arrInput[i] == '>') {
                foundCount--;
            }
            if (arrInput[i] == split && foundCount == 0) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            result = new String[1];
            result[0] = sp;
            return result;
        }
        result = new String[2];
        result[0] = sp.substring(0, index);
        result[1] = sp.substring(index + 1);
        return result;
    }

    public static void main(String[] args) {
        String s = "java.util.List<java.util.Map<java.lang.String,java.lang.String>>";
        String v = "[{\"k11\":\"v11\",\"k12\":\"v12\"},{\"k22\":\"v22\",\"k21\":\"v21\"}]";
        //String s = "java.util.Map<java.lang.String,java.lang.String>";
//        String v = "[['l11','l12'],['l21','l22']]";

        //String v = "{'ss':'vv'}";
        try {
            TypeReferenceMogu t = new TypeReferenceMogu(s);
            Object oMy = com.other.JSON.parseObject2(v, t);
            //Object o = JSON.parseObject(v, new TypeReference<Map>(){});
            System.out.println(t.getType());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("end");
        }

//        List<List<String>> l = new LinkedList<List<String>>();
//
//        List<String> l1 = new LinkedList<String>();
//        l1.add("l11");
//        l1.add("l12");
//
//        List<String> l2 = new LinkedList<String>();
//        l2.add("l21");
//        l2.add("l22");
//
//        l.add(l1);
//        l.add(l2);
//

//        List<Map<String, String>> l = new LinkedList<Map<String, String>>();
//
//        Map<String, String> m1 = new HashMap<String, String>();
//        m1.put("k11","v11");
//        m1.put("k12", "v12");
//
//        Map<String, String> m2 = new HashMap<String, String>();
//        m2.put("k21", "v21");
//        m2.put("k22", "v22");
//
//        l.add(m1);
//        l.add(m2);
//        System.out.println(JSON.toJSONString(l));
    }
}
