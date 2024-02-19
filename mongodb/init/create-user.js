// db名・一般ユーザー・パスワードを環境変数より取得
const BROWNIE_ATELIER_MONGO__MONGO_USE_DB = process.env.BROWNIE_ATELIER_MONGO__MONGO_USE_DB;
const BROWNIE_ATELIER_MONGO__MONGO_USER = process.env.BROWNIE_ATELIER_MONGO__MONGO_USER;
const BROWNIE_ATELIER_MONGO__MONGO_PASS = process.env.BROWNIE_ATELIER_MONGO__MONGO_PASS;

// 上記のdbの初期セットアップを実施（db・ユーザー作成）
db = db.getSiblingDB(BROWNIE_ATELIER_MONGO__MONGO_USE_DB);
db.createUser({
  user: BROWNIE_ATELIER_MONGO__MONGO_USER,
  pwd: BROWNIE_ATELIER_MONGO__MONGO_PASS,
  roles: [
    { role : "readWrite", db : BROWNIE_ATELIER_MONGO__MONGO_USE_DB },
  ]
});