# gokurou

3日で1億ページクロールしたい。  
つまり500crawl/secを目指すぞ。

# 進捗

2019/10/05時点ではMySQL, Redis, S3が必要。`docker-compose.yml`でバチっとすれば全部立ち上がる。  
(ただしS3関連のために`minio/minio`を用いており、これのバケット作成は手動で行う必要あり)  
`go run cmd/gokurou/gokurou.go`すればCLIツールがビルドされ実行されるので、後はそれに設定ファイルを渡して実行する。  
設定ファイルは、`docker-compose`で立ち上がるコンテナに合わせた設定のサンプルを`configs/config.sample.json`としてコミットしている。

```
$ go run cmd/gokurou/gokurou.go -c PATH
NAME:
   gokurou - Let's crawl web!

USAGE:
   gokurou [global options] command [arguments...]

VERSION:
   0.0.1

COMMANDS:
   seeding  Seeding initial URL
   crawl    Start to crawl
   reset    Reset all data(exclude 'artifact')
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --config PATH, -c PATH  configuration file PATH
   --help, -h              show help
   --version, -v           print the version
```