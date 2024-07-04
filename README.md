# sqlyte-db

A simple database like SQLite that can do stuff. It's not a basic one but rather intermediate-level project. Clearly written in C

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
  ./sqlyte-db <database_name>
  ```

- For now the database supports 2 SQL commands:

  - `insert` -- inserts / updates data into the database
  - `select` -- selects data from the database( _only supports `*` for now meaning it selects all data and prints it out_ )

  - `.exit` -- exits the database

  - The database is fully persistent, meaning that you can exit the database and come back to it later and the data will still be there.

  To get more help on how to use the database, type `.help` in the database shell.

  - You can also view it in action [here](https://asciinema.org/a/663557) 
  [![asciicast](https://asciinema.org/a/663557.svg)](https://asciinema.org/a/663557)
  _if you don't want to clone the repository_.

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

  Make sure you're at the root of the project directory when you run the command above.

## NOTE

- I've attached a compiled binary for Linux users. If you're on a different platform, you'll have to compile the source code yourself. The code is Unix-based and may not work on `Windows`( _unless you run `./compile.sh` in a WSL environment, of course_ ). It can compile on `MacOS` but I haven't tested it yet but it **should** work. In case you have forgotten how to compile the source code, refer to the `Usage` section above.
