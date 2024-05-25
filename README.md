# sqlyte-db

A relational database like SQLite that can do stuff. It's not a basic one but rather intermediate-level project. Clearly written in C

## Usage

- Clone the repository

  ```sh
  git clone https://github.com/winterrdog/sqlyte-db.git
  ```

- Compile the source code

  ```sh
  ./compile.sh
  ```

- Run the executable
  ```sh
  ./sqlyte-db
  ```

## Testing

- Make sure you have `node.js` installed on your machine
- Also make sure you have `jest` installed _globally_

  ```sh
  npm install -g jest
  ```

- Only after is when you can test the database using `node.js`' `jest` like so:

  ```sh
  jest
  ```
