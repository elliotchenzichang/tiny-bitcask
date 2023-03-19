## tiny-bitcask

tiny-bitcask是对[bitcask论文](https://riak.com/assets/bitcask-intro.pdf)的简单实现，旨在提供用Go实现简单kv存储引擎的参考。后来我想把这个项目作为实现我各种想法的试验田。所以会持续迭代这个项目，迭代的结果就是master分支。如果想看简单版本的实现可以通过以下方式拉取代码，并且切换到demo分支。

````shell
git clone git@github.com:elliotchenzichang/tiny-bitcask.git
cd tiny-bitcask
git chckout demo
````

另外我想实现做的实验和实现的想法会记录在项目TODO中。并切换分支进行相关迭代，有效果的部分会合并进入master分支。实践的相关文章会列在文章列表上。感谢各位的关注，希望各位都能从中学到一些东西。欢迎star，欢迎提PR。

## Todo

- [ ] 实现HintFile
- [ ] 探究对map的优化
- [ ]  实现version control

## 文章list

1. [基于Bitcask实现简单的kv存储详细讲解](https://mp.weixin.qq.com/s?__biz=Mzg5MzU5NzQxMA==&mid=2247483844&idx=1&sn=2fc13cf8ce7c465dbd08690c56eaba69&chksm=c02d2249f75aab5f0955377c6ed29f8529c4a5f18bd53f27ab4cd88b85b3792af370f8a378ab#rd)

## 个人

下面是本人微信公众号，欢迎关注

![image](https://user-images.githubusercontent.com/92676541/226180799-973944bd-5c75-4a9b-8226-6c7e6e465d19.png)
