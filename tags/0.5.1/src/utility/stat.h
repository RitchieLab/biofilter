#ifndef STAT_H
#define STAT_H 
//
// C++ Implementation: stat.h
//
// Description: Various Statistics calculations that have somewhat general applicability
//
//
// Author: Eric Torstenson <torstenson@chgr.mc.vanderbilt.edu>, (C) Marylyn Ritchie 2007
//
// Copyright: See COPYING file that comes with this distribution
//
//
#include "utility/random.h"
#include <math.h>
size_t PoissonEventCount(Utility::Random& rnd, double labmda);
/**
 * Various statistics calculations that have somewhat general applicability
 * @param rnd This is the random number generator to be used
 * @param poissonLambda The lambda to be used
 */
inline
size_t PoissonEventCount(Utility::Random& rnd, double lambda) {
	double p=exp(-lambda), 
		g=p, 
		u=rnd.drand();

	size_t k=0;
	while (u>g)		{
		p*=(lambda/(double)(++k));
		g+=p;
	}
	return k;
};

#endif //STAT_H
