def prime():
	a = int(input('enter a prime no.'))
	for i in range(2,a):
		if a%i==0:
			return 'not prime'
	else:
		return 'prime'
