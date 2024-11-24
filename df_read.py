# Databricks notebook source
print(";el")

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create a dataframe").getOrCreate()

data = [(1, "Suresh"), (2, "ravi")]
columns = ["id", "Name"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

def my_function():
    print("hello")

# COMMAND ----------

def add(a, b):
    return  a+b


# COMMAND ----------

result = add(5, 5)

# COMMAND ----------

me = add(10, 23)
print(me)

# COMMAND ----------

def suresh_add(a, b, c, d):
    
    return a+b-c+d



# COMMAND ----------

result = suresh_add(10,11, 23, 24)

# COMMAND ----------

print(result)

# COMMAND ----------

############################                      Level 1 : Basic Function
# Function to print "Hello, world!"
def my_function():
    print("Hello, world!")

# Call the basic function
my_function()

############################                     Level 2 : Function with Parameters
# Function to greet a user by name
def suresh(name):
    print(f"Hello, {name}!")

# Call the 'suresh' function
suresh("ravi")

#############################                      Level 3 : Return Values
# Function to add two numbers
def suresh_add(a, b):
    """ 
    This function adds two numbers.
    Parameters:
    a (int/float): The first number to add.
    b (int/float): The second number to add.

    Returns:
    int/float: The sum of a and b.
    """
    return a + b

# Example usage of suresh_add
result = suresh_add(10, 20)
print(f"The sum is: {result}")

############################                     Level 4 : Default Parameters
# Function with a default parameter
def suresh_name(name="Guest"):
    print(f"Hello, {name}!")

# Call the function with the default parameter
suresh_name()  # Uses default value 'Guest'
suresh_name("John")  # Uses passed value 'John'

#############################                      Level 5 : Docstrings
def add(a, b):
    """
    This function adds two numbers
    
    """

    return a + b

#############################                      Level 6 : Variable Scope

global_var = 10

def some_function():
    local_var = 5
    print(global_var + local_var)

some_function()

#############################                      Level 7 : Recursion

def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

# Example usage of factorial function
result = factorial(5)
print(f"Factorial of 5 is: {result}")

#############################                      Level 8 : Lambda Functions

double = lambda x: x * 2
result = double(3)
print(f"Double of 3 is: {result}")

#############################                      Level 9 : Function Decorators

def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")
        func()
        print("Something is happening after the function is called.")
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

# Call the decorated function
say_hello()

#############################                      Advanced Functions: Level 10 - Higher-Order Functions

# Higher-Order Function Example
def apply_function(f, x):
    return f(x)

# Function to square a number
def square(n):
    return n * n

# Applying the higher-order function
result = apply_function(square, 4)
print(f"Square of 4 is: {result}")

#############################                      Advanced Functions: Level 11 - Nested Functions

# Nested Function Example
def outer_function(x):
    def inner_function(y):
        return y + 10
    return inner_function(x)

# Calling the nested function
result = outer_function(5)
print(f"Result of nested function is: {result}")

#############################                      Advanced Functions: Level 12 - Partial Functions

from functools import partial

# Partial Function Example
def multiply(a, b):
    return a * b

# Create a new function that multiplies by 2
double_multiply = partial(multiply, 2)

# Calling the partial function
result = double_multiply(5)
print(f"Multiplying 2 by 5 gives: {result}")

#############################                      Advanced Functions: Level 13 - Function Caching

from functools import lru_cache

# Function caching with LRU Cache
@lru_cache(maxsize=None)  # Cache without size limit
def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)

# Example usage of cached Fibonacci function
result = fibonacci(30)
print(f"Fibonacci of 30 is: {result}")
