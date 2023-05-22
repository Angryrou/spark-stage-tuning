# Get the current directory name
current_dir=$(basename "$PWD")

# Assert that the current directory is "script"
if [ "$current_dir" != "script" ]; then
  echo "The current directory is not correct: $current_dir. Please navigate to benchmark-res/script."
  exit 1
fi

# Check if the argument is provided
if [ -z "$1" ]; then
    echo "Please provide an argument."
    exit 1
fi

# Store the value of the argument in a variable
OS="$1"
# Check the value of the argument using if-else
if [ "$OS" == "LINUX" ]; then
  echo "Not implemented yet"
elif [ "$OS" == "MACOS" ]; then
  mkdir -p ../dataset-gen
  cd ../dataset-gen
  git clone --depth 1 git@github.com:Angryrou/tpcds-kit.git
  cd tpcds-kit/tools
  make OS=$OS
  cd .. # to tpcds-kit
  bash validate_sparksql_gen.sh # should not be any output.
  cd .. # to tpcds-kit/..
  echo "Done"
else
  echo "Invalid OS argument: $OS"
  exit 1
fi