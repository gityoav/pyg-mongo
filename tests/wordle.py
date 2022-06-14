import requests
from bs4 import BeautifulSoup
import numpy as np
req = requests.get('https://github.com/charlesreid1/five-letter-words/blob/master/sgb-words.txt')
soup = BeautifulSoup(req.text, features='lxml')
words = sorted(set([word.text for word in soup.findChildren('td') if word.text]))

def check(guess):
    if len(guess)!=5:
        print('please enter a valid word of length five. Word %s has length %s'%(guess, len(guess)))
        return False
    elif guess.lower() not in words:
        print('please enter a valid word of length five. Word %s is not in dictionary'%guess)
        return False
    else:
        return True


def compare(word, guess):
    """

    Parameters
    ----------
    word : string
        The wordle word chosen.
    guess : string
        the guess of the user.

    Returns
    -------
    string
        a string of x, o or ! depending if letter is not in word, different place or exact match.

    """
    word = word.lower()
    guess = guess.lower()
    return ''.join(['x' if letter not in word else '!' if letter == word[i] else 'o' for i, letter in enumerate(guess)])


def single_game(word):
    """
    
    Parameters
    ----------
    word : string
        The wordle word.

    Returns
    -------
    i : int
        DESCRIPTION.

    """
    print('welcome to wordle... please enter your guess:') #it interacts and asks the user for its guess
    for i in range(7): #the amount of guesses you put in
        ok = False
        while not ok:
            guess = input() # ask the user for input
            ok = check(guess)
        score = compare(word, guess)# compares the word guess to the word every time
        print(score)  # show the user its score
        if guess == word: # if the guess is the word:
            print('well done!') # tells user "well done"
            return i #tells user its score
    print('too bad, the word was:', word) #if you didnt win, you get "too bad, it will output the actual word"
    return 


play = 'y'
while play.lower()[0] == 'y':
    n = np.random.randint(len(words))    
    word = words[n]
    single_game(word)
    print('play another (y/n)?')
    play = input()



#######################################

def single_game_guess_the_number(number):
    number = 25
    ## you have 6 guesses to guess the number
    ## when you guess, it will tell you higher/lower/exactly!
        if input() is higher than number print 'Your guess is higher than the number'
        if input() is lower than number print 'Your guess is lower than the number'
print('welcome to Guess The Number :D . Please enter a guess')    
    for i in range (6):
        guess = input() 
        score = compare(number,guess)


i = input()

i = int(i)

if i>number:
    print('Your guess %s is higher than the number'%i)
elif i<number:
    print('Your guess %s is lower than the number'%i)
else:
    print('well done')

i = input()







