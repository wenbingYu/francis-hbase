package com.ipinyou.hbase.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlCommonUtils {

	public static String getDomain(String url) {
		if (isEmpty(url)) {
			return "";
		}
		int k, cnt = 0;
		char ch = '/';
		char ch1 = '?';
		for (k = 0; k < url.length(); k++) {
			if (ch == url.charAt(k) || ch1 == url.charAt(k)) {
				cnt++;
			}
			if (cnt == 3)
				break;
		}
		url = url.substring(0, k);
		Pattern pattern = Pattern
				.compile("^http[s]?://([^\\/:]+)(:[\\d+])?(\\/[\\w-\\.\\/\\?\\%&=#]*)?$");

		Matcher matcher = pattern.matcher(url);
		String result = null;
		/* 域名后缀可以不断添加 */
		String ext = "com,net,org,gov,edu,info,name,int,mil,arpa,asia,biz,pro,coop,aero,museum,ac,ad,ae,af,ag,ai,al,am,an,ao,aq,ar,as,at,au,aw,az,ba,bb,bd,be,bf,bg,bh,bi,bj,bm,bn,bo,br,bs,bt,bv,bw,by,bz,ca,cc,cf,cg,ch,ci,ck,cl,cm,cn,co,cq,cr,cu,cv,cx,cy,cz,de,dj,dk,dm,do,dz,ec,ee,eg,eh,es,et,ev,fi,fj,fk,fm,fo,fr,ga,gb,gd,ge,gf,gh,gi,gl,gm,gn,gp,gr,gt,gu,gw,gy,hk,hm,hn,hr,ht,hu,id,ie,il,in,io,iq,ir,is,it,jm,jo,jp,ke,kg,kh,ki,km,kn,kp,kr,kw,ky,kz,la,lb,lc,li,lk,lr,ls,lt,lu,lv,ly,ma,mc,md,me,mg,mh,ml,mm,mn,mo,mp,mq,mr,ms,mt,mv,mw,mx,my,mz,na,nc,ne,nf,ng,ni,nl,no,np,nr,nt,nu,nz,om,pa,pe,pf,pg,ph,pk,pl,pm,pn,pr,pt,pw,py,qa,re,ro,ru,rw,sa,sb,sc,sd,se,sg,sh,si,sj,sk,sl,sm,sn,so,sr,st,su,sy,sz,tc,td,tf,tg,th,tj,tk,tm,tn,to,tp,tr,tt,tv,tw,tz,ua,ug,uk,us,uy,va,vc,ve,vg,vn,vu,wf,ws,ye,yu,za,zm,zr,zw";

		while (matcher.find()) {
			result = matcher.group(1);
		}
		if (isEmpty(result) || !result.contains(".")) {
			return "";
		}
		List<String> list = Arrays.asList(result.split("\\."));

		// 如果domain总段数为2，而且符合域名规则，直接返回
		// 比如：org.cn, qq.com
		if (list.size() == 2) {
			if (ext.indexOf(list.get(1)) == -1) {
				// throw new IllegalArgumentException();
				return "";
			} else {
				return join(list);
			}
		}

		// 忽略最后一段，无论该字段是什么都要加入最后结果
		for (int i = list.size() - 2; i >= 0; i--) {
			if ("www".equals(list.get(i))) {
				result = join(list.subList(i + 1, list.size()));
				break;
			}

			if (ext.indexOf(list.get(i)) == -1) {
				result = join(list.subList(i, list.size()));
				break;
			}

		}

		return result;
	}

	public static String join(List<String> l) {
		StringBuffer sb = new StringBuffer();
		Iterator<String> i = l.iterator();
		boolean isNull = i.hasNext();
		while (isNull) {
			String o = i.next();
			sb.append(o);
			isNull = i.hasNext();
			if (isNull) {
				sb.append(".");
			}
		}

		return sb.toString();
	}

	public static Boolean isEmpty(String cs) {
		return cs == null || cs.length() == 0;
	}

}
