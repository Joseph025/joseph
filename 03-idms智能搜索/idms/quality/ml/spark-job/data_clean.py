# -*- coding: utf-8 -*-

import re
import sys

def cln_html_esc(content):
    '''去除HTML中的特殊字符'''
    content = re.sub('<.+?>',' ',content)
    content = re.sub('&nbsp;',' ',content)
    content = re.sub('&quot;',' ',content)
    content = re.sub('&lt;',' ',content)
    content = re.sub('&gt;',' ',content)
    content = re.sub('&amp;',' ',content)
    content = re.sub('\s+',' ',content)
    return content

def cln_write_model(text,mfile=None):
    ''' 清除描述/模板信息'''
    if mfile is None:
        mfile = 'template_clip.txt'
    with open(mfile,'r',encoding='utf-8') as ofile:
        content = ofile.read()
        content = content.replace("(",'（')
        content = content.replace(")",'）')
        content = content.replace('\\','/')
        sentences = content.split('\n')
    for sentence in sentences:
        text = text.replace(sentence,' ')
        text = re.sub('\n','',text)
    return text.replace('\r','').replace('\n','')


def remove_long_char(text,minlong=17):
    '''剔除连续非中文字符大于minlong的字符'''
    text = str(text)
    if len(text)<minlong:minlong=len(text)
    pattern = r"[\x00-\xff]{%s,%s}"%(minlong,len(text))
    lchars = re.findall(pattern,text)
    for lchar in lchars:
        text = text.replace(lchar,'')
    text = re.sub('[：:。,，-－]{2,}',' ',text)
    return text

def remove_title(text):
    '''清除文本中的【】/[]字符内的内容'''
    pat = '【.*?】|\[.*?\]'
    return re.sub(pat,' ',text)

def clean_detail(data):
    data = cln_html_esc(data)
    data = cln_write_model(data)
    data = remove_title(data)
    data = remove_long_char(data)
    return data

def clean_detail_nrlc(data):
    data = cln_html_esc(data)
    data = cln_write_model(data)
    data = remove_title(data)
    return data